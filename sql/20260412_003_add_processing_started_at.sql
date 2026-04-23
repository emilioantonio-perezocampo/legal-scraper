ALTER TABLE public.scraper_documents
    ADD COLUMN IF NOT EXISTS processing_started_at timestamptz;

COMMENT ON COLUMN public.scraper_documents.processing_started_at IS
    'Timestamp when the current embedding attempt entered processing.';
