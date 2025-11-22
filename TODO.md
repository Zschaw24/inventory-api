# TODO: Fix fetch_inventory.py for reliable MongoDB Atlas inserts

- [x] Modify parse_report_to_dataframe to skip rows without valid seller-sku (not null or empty)
- [x] Update upsert_inventory to use bulk_write for active listings upsert
- [x] Update upsert_inventory to use bulk_write for marking missing SKUs as sold
- [x] Add error handling and logging for bulk operations
- [x] Test locally to ensure all rows are processed
