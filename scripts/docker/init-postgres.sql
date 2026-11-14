-- =============================================================================
-- PostgreSQL CDC Initialisation Script
-- Runs automatically when the postgres container starts for the first time.
-- Sets up a sample schema, replication slot, and publication for Debezium.
-- =============================================================================

-- ── Sample schema and tables ─────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS public.customers (
    id          SERIAL PRIMARY KEY,
    name        VARCHAR(100) NOT NULL,
    email       VARCHAR(150) UNIQUE NOT NULL,
    created_at  TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at  TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS public.orders (
    id          SERIAL PRIMARY KEY,
    customer_id INTEGER REFERENCES public.customers(id),
    amount      NUMERIC(12, 2) NOT NULL,
    status      VARCHAR(50) DEFAULT 'pending',
    created_at  TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at  TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- ── Seed data ─────────────────────────────────────────────────────────────────
INSERT INTO public.customers (name, email) VALUES
    ('Alice Smith',  'alice@example.com'),
    ('Bob Jones',    'bob@example.com'),
    ('Carol White',  'carol@example.com')
ON CONFLICT DO NOTHING;

INSERT INTO public.orders (customer_id, amount, status) VALUES
    (1, 99.99,  'completed'),
    (1, 149.00, 'pending'),
    (2, 59.50,  'completed')
ON CONFLICT DO NOTHING;

-- ── Replication slot ──────────────────────────────────────────────────────────
-- Debezium uses this slot to track WAL position.
-- The slot name must match debezium.properties.slot.name in your env file.
SELECT pg_create_logical_replication_slot('debezium_local_slot', 'pgoutput')
WHERE NOT EXISTS (
    SELECT 1 FROM pg_replication_slots WHERE slot_name = 'debezium_local_slot'
);

-- ── Publication ───────────────────────────────────────────────────────────────
-- Publishes all DML changes for all tables.
-- The name must match debezium.properties.publication.name in your env file.
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_publication WHERE pubname = 'debezium_local_publication') THEN
        CREATE PUBLICATION debezium_local_publication FOR ALL TABLES;
    END IF;
END
$$;
