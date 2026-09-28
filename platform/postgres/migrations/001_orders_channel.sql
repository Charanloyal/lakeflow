-- Contract v2: optional sales channel. Additive and nullable, so v1 consumers keep working (BACKWARD_TRANSITIVE).
-- Applied at runtime by `python scripts/lakeflowctl.py migrate` or the Recovery Lab "schema change" action.
ALTER TABLE shop.orders
    ADD COLUMN IF NOT EXISTS channel text CHECK (channel IN ('web', 'mobile', 'store'));
