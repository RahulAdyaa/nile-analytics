# Roadmap - Ecommerce Pipeline

## Milestone 1: Core Implementation

### Phase 1: Database Schema Update
- [ ] Add `age`, `gender` to `Customer`.
- [ ] Add `delivery_time_days`, `returned`, `shipping_cost` to `Sale`.
- [ ] Run migrations.
- **UAT**: Models reflect new fields in `admin` or shell.

### Phase 2: ETL Pipeline Enhancement
- [ ] Update `ETLPipeline` mapping and cleaning logic.
- [ ] Update `ETLPipeline.load_to_db` for new fields.
- **UAT**: Pipeline handles `ecommerce_sales_34500.csv` headers correctly.

### Phase 3: Execution & Validation
- [ ] Run `ingest_ecommerce` command.
- [ ] Verify data integrity in DB.
- **UAT**: 34,500 records processed.
