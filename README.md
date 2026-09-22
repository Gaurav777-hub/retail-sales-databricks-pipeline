# Retail Sales Analytics Pipeline — Azure Databricks

## Architecture

*Diagrams to be added once the full pipeline (Bronze → Silver → Gold → Power BI) is running end-to-end.*

---

## Challenges & Learnings

Building this pipeline surfaced several real-world governance and identity issues that don't show up in tutorial-scale demos — documenting them here because working through them was as valuable as the pipeline itself.

**Choosing managed identity over service principals for Unity Catalog**
Rather than using a service principal with a stored secret, I used an Access Connector for Azure Databricks with a system-assigned managed identity. This is the Microsoft-recommended approach — it removes the need to manage and rotate credentials, and it works with storage accounts that have network firewall rules, which service principals can't access as cleanly.

**Metastores are regional and account-level, not per-workspace**
Unity Catalog allows only one metastore per region per Databricks account. Rather than creating a new one, I reused an existing East US metastore and attached my new workspace to it. This meant reasoning through isolation at the catalog/schema level instead — I namespaced my catalog as `retail_sales_dev` and scoped `GRANT`s accordingly, since catalogs under a shared metastore are visible to every attached workspace.

**Diagnosing a 403 authorization failure systematically**
When creating an external location, permission checks failed on Read/List/Write but succeeded on Path Exists and Hierarchical Namespace Enabled. This distinction was the key clue: the passing checks are control-plane (ARM) operations, while the failing ones are data-plane operations gated by RBAC. That pointed straight at the IAM role assignment rather than the storage account configuration itself, and avoided wasted time re-checking settings that were already correct.

**Understanding the full IAM surface for Auto Loader's file-events feature**
Attempting to enable file events (Event Grid-based file notifications for Auto Loader) surfaced a provisioning error requiring four separate roles on the Access Connector's identity: `Storage Blob Data Contributor`, `Storage Account Contributor`, `EventGrid EventSubscription Contributor`, and `Storage Queue Data Contributor`. Rather than granting broad permissions up front, I deferred file events and used Auto Loader's default directory-listing mode instead — an explicit scope decision to keep the initial build lean, with file events noted as a follow-up optimization.

**Role assignments take time to propagate**
IAM role assignments in Azure aren't instant — propagation can take several minutes, occasionally longer. Several "permission" errors during setup were actually propagation delays, not misconfiguration, which reinforced the value of isolating *what* failed (via Databricks' per-operation Test Connection: Read/List/Write/Delete) before changing anything.

**A shared metastore can silently default to the wrong identity**
After the propagation delay was ruled out, the root cause turned out to be that the Storage Credential was referencing a different access connector (`unity-catalog-access-connector`) than the one I had explicitly created and granted roles to (`dbac-retailsales-dev-eus-001`). Because I attached my workspace to a pre-existing East US metastore rather than creating a new one, Databricks had already provisioned its own default connector for that metastore, and the credential picked it up instead of my purpose-built one. All the IAM roles were correctly assigned — just to an identity the credential wasn't actually using. This reinforced a broader lesson from working with shared, account-level resources: always verify which specific identity a dependent resource is referencing, rather than assuming it points to the one you most recently created. Fixing it meant explicitly re-pointing the Storage Credential at `dbac-retailsales-dev-eus-001`, keeping the project's access chain self-contained and traceable to a connector I provisioned and scoped myself.

**Unity Catalog requires explicit managed locations without Default Storage**
Catalog creation failed with `Metastore storage root URL does not exist`, because the metastore didn't have a default storage root configured — every catalog needs an explicit `MANAGED LOCATION`, not just its schemas. Resolved by pointing the catalog at a subfolder under an existing external location, since schema-level managed locations take precedence for actual data storage.

**Catalogs, not schemas, are the environment isolation boundary in Unity Catalog**
Rather than a single catalog with `dev_bronze`/`prod_bronze`-style schemas, Databricks' recommended pattern is separate catalogs per environment (`retail_sales_dev`, `retail_sales_prod`), each with identical `bronze`/`silver`/`gold` schemas underneath. This keeps `GRANT`s, storage locations, and lineage cleanly separated at the top level, so a dev-side permissions mistake can't expose prod data. Promotion between environments happens by parameterizing notebooks (a `catalog_name` widget/job parameter) rather than renaming or duplicating objects.

**External Location names are Databricks objects, not ADLS container names**
Because `bronze`/`silver`/`gold` were already taken as external location names on the shared metastore, mine were registered as `bronze_dbac`/`silver_dbac`/`gold_dbac` instead. This naming lives entirely inside Databricks — the actual ADLS containers are still plainly named `bronze`/`silver`/`gold`. It's an easy distinction to blur when writing `abfss://` paths in code, since the container name (not the external location name) is what belongs in the path itself.

**A metastore's default storage credential is scoped only to its own internal storage, not to your data**
Every Unity Catalog metastore has a built-in default/root storage credential used purely for its own internal bookkeeping storage (the system-managed `unity-catalog-storage` container Databricks provisions automatically). It is never meant to authorize access to a project's own data. Whenever a path can't be resolved against a properly linked Storage Credential, Unity Catalog silently falls back to this restricted default — producing a `PERMISSION_DENIED` error that referenced the workspace's own credential name, even though every relevant RBAC role and external-location grant was already correctly set up. The fix was creating an explicit Storage Credential (`cred-retail-sales-dev-eus`) tied to my own Access Connector, and re-pointing the external locations at it, since a credential must be deliberately linked end-to-end rather than assumed to resolve correctly by default. This combined with the earlier shared-metastore mix-up into a broader pattern: in Unity Catalog, one Access Connector → one Storage Credential → one or more External Locations → Catalog/Schema managed locations is a full chain, and any object in it can silently point to the wrong thing if not explicitly checked.

---

## Bronze & Silver Layer — Build Learnings

**`cloudFiles.schemaHints` doesn't validate that the column name actually exists in the source**
Passed `InvoiceNo STRING` as a schema hint, assuming that was the real column name — the actual source column was `Invoice`. Auto Loader didn't error on this; it silently added a brand-new, entirely null `InvoiceNo` column alongside the real `Invoice` column, since a hint for a non-matching name is just added as its own nullable field rather than validated against the source. Caught it by noticing the phantom column was 100% null. The fix required more than a code change: Auto Loader had already persisted the wrong inferred schema in `cloudFiles.schemaLocation`, and the Bronze table already had the wrong structure baked in, so a clean fix meant dropping the Bronze table and wiping both the checkpoint and schema location before re-running from scratch. Lesson: always confirm real column names directly from the source (`spark.read.csv(...).printSchema()`, no Auto Loader involved) before writing schema hints, rather than assuming a name.

**A wrong type inference can silently reroute rows into the rescued-data column instead of erroring**
Because `Invoice` wasn't correctly hinted, Auto Loader inferred its type from the data alone, which risked settling on a numeric type given most values are numeric — meaning the `C`-prefixed cancellation rows could have been silently diverted into `_rescued_data` rather than stored properly, with no exception raised. This is the real danger of unvalidated schema assumptions: the pipeline keeps running and looks healthy while quietly losing structure on a subset of rows. Checking `_rescued_data IS NOT NULL` counts became a standard verification step after this.

**Dropping and recreating a Delta table breaks any downstream stream's checkpoint**
After dropping and recreating the Bronze table to fix the schema, the Silver stream (which reads from Bronze via `spark.readStream.table(...)`) couldn't safely resume from its existing checkpoint, since the checkpoint was tracking progress against the old table's transaction history, not the new one's. Fixing an upstream table means resetting every downstream stream's checkpoint too, not just the one that changed.

**Source data can't be trusted to follow one date format**
`to_timestamp` with a single assumed pattern (`M/d/yyyy H:mm`) threw a runtime exception and halted the entire stream when it hit a row formatted as `04-01-11 10:00` (day-month-year, two-digit year) instead. Switched to `try_to_timestamp`, which returns `NULL` for unparseable rows instead of crashing the whole pipeline, paired with an explicit count of resulting nulls as a deliberate data-quality check rather than a silent failure.

**Watermarking exists to bound state size for streaming deduplication, not to filter late data on its own**
`dropDuplicates()` in a streaming context has to remember every key it's ever seen to catch a future duplicate, which grows memory unboundedly over a long-running stream. `withWatermark("InvoiceDate", "3 days")` tells Spark it can safely forget state for rows once 3 days of event-time have passed, bounding that growth. This is a distinct mechanism from a checkpoint: a checkpoint tracks *progress through the stream* (which files/rows have been consumed, for fault-tolerant resume), while a watermark is a *policy inside a stateful operator* governing how long to keep waiting for late-arriving data before discarding state.

**Deliberate data-quality decisions**
- Null `CustomerID`s (a meaningful share of this dataset) are flagged via a `has_customer_id` boolean and replaced with a literal `"UNKNOWN"` placeholder, rather than dropped — preserves total revenue accuracy and gives `dim_customer` a real join target instead of `NULL` foreign keys breaking inner joins later.
- Cancelled orders (`Invoice` starting with `C`) are flagged via `is_cancelled` rather than filtered out — they're legitimate business events, not bad data, and worth analyzing separately (e.g. cancellation rate by month) rather than discarding.
- Deduplication key is the combination of `Invoice`, `StockCode`, `CustomerID`, `Quantity`, and `InvoiceDate` — `Invoice` alone isn't unique, since one invoice legitimately has many line items.

---

## CI/CD Approach

This project uses **Databricks Asset Bundles (DABs)** — the current Databricks-recommended way to define jobs, notebook paths, and environment-specific configuration as code in a `databricks.yml` file, rather than manually managing notebooks through the workspace UI.

**Environments as bundle targets**: `dev` and `prod` targets in `databricks.yml` each map to a different Unity Catalog catalog (`retail_sales_dev` / `retail_sales_prod`) via a `catalog_name` variable, so the same notebook code is parameterized rather than duplicated per environment.

**Pipeline**: GitHub Actions workflows (`.github/workflows/`) run `databricks bundle validate` and `databricks bundle deploy` on push, authenticating as a Databricks **service principal** (via a GitHub Secret token) rather than a personal login — least-privilege by design, and avoids tying deployments to any one person's credentials.

**Scope for this project**: automatic validate + deploy to `dev` on push to `main`; `prod` deploy left manual (`workflow_dispatch`) rather than fully automated, since there's no real production traffic to justify it — the goal here is demonstrating the promotion pattern, not running a 24/7 pipeline.

Official reference: https://learn.microsoft.com/en-us/azure/databricks/dev-tools/bundles/ci-cd

---

*Add setup instructions, notebook descriptions, and Power BI screenshots here once the pipeline is fully running end-to-end.*
