# dbt PR Template: 2nd Priority - Data Quality Test Coverage

**Model name(s):**  
**Related ticket / work item:**  

## Test Cases for Data Quality Checks

### A. Core dbt tests

- [ ] Add dbt tests for key columns:
  - [ ] `not_null`
  - [ ] `unique` (for primary keys or business keys)
  - [ ] `accepted_values` (for status or type flags, enums)
  - [ ] `relationships` (foreign key relationships to parent tables)

### B. Custom tests

- [ ] Add custom tests where logic is complex (for example calculated metrics, business rules).
- [ ] Ensure tests are defined in `.yml` files and linked to the correct models.

### C. Execution and tagging

- [ ] Run `dbt test` and confirm all critical tests pass.
- [ ] Tag tests or models (for example `tags: ['dq', 'critical']`) for focused test runs.
- [ ] Document what each important test is protecting (business rationale).

## Notes

- List of critical tests and what they protect:
- Any failing or flaky tests and next steps:
