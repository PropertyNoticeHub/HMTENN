-- Enable Row-Level Security
ALTER TABLE businesses ENABLE ROW LEVEL SECURITY;

-- Remove old policy if needed
DROP POLICY IF EXISTS "Allow public insert" ON businesses;
DROP POLICY IF EXISTS "Allow anonymous inserts" ON businesses;
DROP POLICY IF EXISTS "Allow anonymous reads" ON businesses;

-- Allow anonymous INSERTs (e.g., from Python upload script)
CREATE POLICY "Allow anonymous inserts"
ON businesses
FOR INSERT
TO anon
WITH CHECK (
  true
);

-- (Optional) Allow anonymous SELECTs (e.g., for frontend display)
CREATE POLICY "Allow anonymous reads"
ON businesses
FOR SELECT
TO anon
USING (
  true
);
