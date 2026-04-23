create table if not exists public.scraper_document_aliases (
    document_id uuid not null references public.scraper_documents(id) on delete cascade,
    source_type text not null,
    alias_type text not null,
    alias_value text not null,
    created_at timestamptz not null default now()
);

create unique index if not exists scraper_document_aliases_source_alias_idx
    on public.scraper_document_aliases (source_type, alias_value);

create index if not exists scraper_document_aliases_document_id_idx
    on public.scraper_document_aliases (document_id);
