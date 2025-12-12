# dbt PR Template: 3rd Priority - Documentation

**Model name(s):**  
**Related ticket / work item:**  

## A. Model and Project Documentation

### Descriptions

- [ ] Add descriptions for:
  - [ ] Each model.
  - [ ] Key columns (especially primary keys, foreign keys, metrics, business flags).

### Business logic

- [ ] Explain business logic, not just technical logic:
  - [ ] What does this model represent.
  - [ ] How should it be used.
  - [ ] What filters or joins are recommended.
  - [ ] What filters or joins are risky or should be avoided.

### Docs and lineage

- [ ] Use docs blocks or markdown (if using dbt docs site) where needed for complex flows.
- [ ] Keep lineage clear:
  - [ ] Model dependencies are meaningful and readable.
  - [ ] Source freshness and assumptions are described.

### dbt docs verification

- [ ] Regenerate dbt docs and verify that:
  - [ ] Lineage graph looks correct.
  - [ ] Descriptions render properly.
  - [ ] Important models are easy to find and search.

## Notes

- Any caveats for consumers:
- Links to rendered docs or screenshots (if applicable):
