-- Run during fresh PostgreSQL initialization and again from setup.sh for
-- existing volumes. Historical lines remain NULL because their true cost at
-- purchase time cannot be reconstructed from the current product row.
ALTER TABLE orderdetails
    ADD COLUMN IF NOT EXISTS unit_cost_at_order NUMERIC(15, 2);

CREATE OR REPLACE FUNCTION snapshot_orderdetail_unit_cost()
RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    SELECT p.unit_cost INTO NEW.unit_cost_at_order
    FROM products AS p WHERE p.id = NEW.product_id;
    RETURN NEW;
END;
$$;

DROP TRIGGER IF EXISTS trg_orderdetail_unit_cost ON orderdetails;
CREATE TRIGGER trg_orderdetail_unit_cost
BEFORE INSERT OR UPDATE OF product_id ON orderdetails
FOR EACH ROW EXECUTE FUNCTION snapshot_orderdetail_unit_cost();
