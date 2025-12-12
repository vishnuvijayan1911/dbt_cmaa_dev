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

## Notes

- Summary of analysis:
- Known limitations or assumptions:
