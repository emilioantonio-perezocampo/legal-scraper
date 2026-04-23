BEGIN;

CREATE TABLE IF NOT EXISTS public.scraper_document_aliases (
    document_id uuid NOT NULL REFERENCES public.scraper_documents(id) ON DELETE CASCADE,
    source_type text NOT NULL,
    alias_type text NOT NULL,
    alias_value text NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now()
);

CREATE UNIQUE INDEX IF NOT EXISTS scraper_document_aliases_source_alias_uidx
    ON public.scraper_document_aliases (source_type, alias_value);

CREATE INDEX IF NOT EXISTS scraper_document_aliases_document_id_idx
    ON public.scraper_document_aliases (document_id);

ALTER TABLE public.scraper_documents
    DROP CONSTRAINT IF EXISTS scraper_documents_embedding_status_check;

ALTER TABLE public.scraper_documents
    ADD CONSTRAINT scraper_documents_embedding_status_check
    CHECK (
        embedding_status IN (
            'pending',
            'processing',
            'completed',
            'failed',
            'awaiting_source_file'
        )
    );

COMMIT;
