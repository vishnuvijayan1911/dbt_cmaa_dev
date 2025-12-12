# dbt PR Template: 1st Priority - Core Model Validation

**Model name(s):**  
**Related ticket / work item:**  

## A. Compatible and Performant Code Conversion

- [ ] Confirm SQL compiles successfully in dbt (no Jinja or macro errors).
- [ ] Ensure code follows dbt style and project conventions (naming, schemas, folder structure).
- [ ] Verify logic is equivalent to the original Synapse (or source) implementation.

**Performance best practices**

- [ ] Avoid unnecessary `SELECT *`.
- [ ] Use incremental models where appropriate.
- [ ] Minimize expensive joins or cross joins.
- [ ] Confirm materialization strategy is appropriate (`view`, `table`, `incremental`, etc).

## B. Data Sanity Checks (vs Synapse Output)

- [ ] Row counts match, or differences are understood and justified.
- [ ] Key aggregates match (for example sums, counts, distinct counts for important measures).
- [ ] Spot-check sample records by joining dbt output to Synapse output on key fields.

**Validate distributions**

- [ ] Date ranges are as expected.
- [ ] Categorical value distributions look reasonable.
- [ ] Outliers or null patterns are understood and acceptable.
- [ ] Confirm any known business KPIs are unchanged, or changes are explained.

## 2nd Priority – Data Quality Test Coverage

### A. Core dbt tests

- [ ] Add dbt tests for key columns:
  - [ ] `not_null`
  - [ ] `unique` (for primary keys or business keys)
  - [ ] `accepted_values` (for status or type flags, enums)
  - [ ] `relationships` (foreign key relationships to parent tables)

### B. Custom tests

- [ ] Add custom tests where logic is complex (for example calculated metrics, business rules).
- [ ] Ensure tests are defined in `.yml` files and linked to the correct models and modules.

### C. Execution and tagging

- [ ] Run `dbt test` and confirm all critical tests pass.
- [ ] Tag tests or models (for example `tags: ['dq', 'sales']`) for focused modulewise test runs.
- [ ] Document what each important test is protecting (business rationale if any).


## Notes

- Summary of analysis:
- Known limitations or assumptions:
