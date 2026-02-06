-- 001_initial_schema.sql
-- Initial database schema for Bulk Import/Export system

-- Enable UUID extension
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

-- ============================================
-- Main Tables
-- ============================================

-- Users table
CREATE TABLE IF NOT EXISTS users (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    name VARCHAR(255) NOT NULL,
    email VARCHAR(255) NOT NULL UNIQUE,
    role VARCHAR(50) NOT NULL CHECK (role IN ('admin', 'author', 'reader')),
    active BOOLEAN NOT NULL DEFAULT true,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- Create indexes for users
CREATE INDEX IF NOT EXISTS idx_users_email ON users(email);
CREATE INDEX IF NOT EXISTS idx_users_role ON users(role);
CREATE INDEX IF NOT EXISTS idx_users_active ON users(active);
CREATE INDEX IF NOT EXISTS idx_users_created_at ON users(created_at);

-- Articles table
CREATE TABLE IF NOT EXISTS articles (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    title VARCHAR(500) NOT NULL,
    slug VARCHAR(500) NOT NULL UNIQUE,
    body TEXT NOT NULL,
    author_id UUID NOT NULL REFERENCES users(id),
    status VARCHAR(50) NOT NULL CHECK (status IN ('draft', 'published')) DEFAULT 'draft',
    published_at TIMESTAMP WITH TIME ZONE,
    tags JSONB DEFAULT '[]',
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    CONSTRAINT chk_published_at CHECK (
        (status = 'draft' AND published_at IS NULL) OR
        (status = 'published' AND published_at IS NOT NULL)
    )
);

-- Create indexes for articles
CREATE INDEX IF NOT EXISTS idx_articles_slug ON articles(slug);
CREATE INDEX IF NOT EXISTS idx_articles_author_id ON articles(author_id);
CREATE INDEX IF NOT EXISTS idx_articles_status ON articles(status);
CREATE INDEX IF NOT EXISTS idx_articles_published_at ON articles(published_at);
CREATE INDEX IF NOT EXISTS idx_articles_created_at ON articles(created_at);

-- Comments table
CREATE TABLE IF NOT EXISTS comments (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    article_id UUID NOT NULL REFERENCES articles(id),
    user_id UUID NOT NULL REFERENCES users(id),
    body TEXT NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- Create indexes for comments
CREATE INDEX IF NOT EXISTS idx_comments_article_id ON comments(article_id);
CREATE INDEX IF NOT EXISTS idx_comments_user_id ON comments(user_id);
CREATE INDEX IF NOT EXISTS idx_comments_created_at ON comments(created_at);

-- ============================================
-- Job Management Tables
-- ============================================

-- Jobs table for tracking import/export operations
CREATE TABLE IF NOT EXISTS jobs (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    type VARCHAR(50) NOT NULL CHECK (type IN ('import', 'export')),
    resource VARCHAR(50) NOT NULL CHECK (resource IN ('users', 'articles', 'comments')),
    status VARCHAR(50) NOT NULL CHECK (status IN ('pending', 'processing', 'completed', 'failed')) DEFAULT 'pending',
    idempotency_key VARCHAR(255) UNIQUE,
    file_path VARCHAR(1024),
    file_url VARCHAR(1024),
    file_format VARCHAR(50),
    total_records INTEGER DEFAULT 0,
    processed_records INTEGER DEFAULT 0,
    successful_records INTEGER DEFAULT 0,
    failed_records INTEGER DEFAULT 0,
    error_message TEXT,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    started_at TIMESTAMP WITH TIME ZONE,
    completed_at TIMESTAMP WITH TIME ZONE,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- Create indexes for jobs
CREATE INDEX IF NOT EXISTS idx_jobs_type ON jobs(type);
CREATE INDEX IF NOT EXISTS idx_jobs_status ON jobs(status);
CREATE INDEX IF NOT EXISTS idx_jobs_resource ON jobs(resource);
CREATE INDEX IF NOT EXISTS idx_jobs_created_at ON jobs(created_at);
CREATE INDEX IF NOT EXISTS idx_jobs_idempotency_key ON jobs(idempotency_key);

-- Job errors table for storing per-record errors
CREATE TABLE IF NOT EXISTS job_errors (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    job_id UUID NOT NULL REFERENCES jobs(id) ON DELETE CASCADE,
    row_number INTEGER NOT NULL,
    record_identifier VARCHAR(255),
    field_name VARCHAR(255),
    error_code VARCHAR(100) NOT NULL,
    error_message TEXT NOT NULL,
    raw_data TEXT,
    field_value TEXT,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- Create indexes for job_errors
CREATE INDEX IF NOT EXISTS idx_job_errors_job_id ON job_errors(job_id);
CREATE INDEX IF NOT EXISTS idx_job_errors_row_number ON job_errors(job_id, row_number);

-- Idempotency keys table
CREATE TABLE IF NOT EXISTS idempotency_keys (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    idempotency_key VARCHAR(255) NOT NULL UNIQUE,
    job_id UUID NOT NULL REFERENCES jobs(id),
    status_code INTEGER NOT NULL,
    response_body JSONB,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    expires_at TIMESTAMP WITH TIME ZONE DEFAULT (NOW() + INTERVAL '24 hours')
);

-- Create indexes for idempotency_keys
CREATE INDEX IF NOT EXISTS idx_idempotency_keys_key ON idempotency_keys(idempotency_key);
CREATE INDEX IF NOT EXISTS idx_idempotency_keys_expires_at ON idempotency_keys(expires_at);

-- ============================================
-- Staging Tables for Import Processing
-- ============================================

-- Staging table for users
CREATE TABLE IF NOT EXISTS staging_users (
    staging_id SERIAL PRIMARY KEY,
    job_id UUID NOT NULL REFERENCES jobs(id) ON DELETE CASCADE,
    row_number INTEGER NOT NULL,
    id VARCHAR(255),
    email VARCHAR(255),
    name VARCHAR(255),
    role VARCHAR(50),
    active VARCHAR(50),
    created_at VARCHAR(255),
    updated_at VARCHAR(255),
    validation_error VARCHAR(255),
    is_valid BOOLEAN DEFAULT true,
    is_duplicate BOOLEAN DEFAULT false,
    processed BOOLEAN DEFAULT false
);

-- Create indexes for staging_users
CREATE INDEX IF NOT EXISTS idx_staging_users_job_id ON staging_users(job_id);
CREATE INDEX IF NOT EXISTS idx_staging_users_email ON staging_users(email);
CREATE INDEX IF NOT EXISTS idx_staging_users_is_valid ON staging_users(job_id, is_valid);
CREATE INDEX IF NOT EXISTS idx_staging_users_staging_id ON staging_users(staging_id);

-- Staging table for articles
CREATE TABLE IF NOT EXISTS staging_articles (
    staging_id SERIAL PRIMARY KEY,
    job_id UUID NOT NULL REFERENCES jobs(id) ON DELETE CASCADE,
    row_number INTEGER NOT NULL,
    id VARCHAR(255),
    slug VARCHAR(500),
    title VARCHAR(500),
    body TEXT,
    author_id VARCHAR(255),
    tags VARCHAR(255),
    published_at VARCHAR(255),
    status VARCHAR(50),
    validation_error VARCHAR(255),
    is_valid BOOLEAN DEFAULT true,
    is_duplicate BOOLEAN DEFAULT false,
    processed BOOLEAN DEFAULT false
);

-- Create indexes for staging_articles
CREATE INDEX IF NOT EXISTS idx_staging_articles_job_id ON staging_articles(job_id);
CREATE INDEX IF NOT EXISTS idx_staging_articles_slug ON staging_articles(slug);
CREATE INDEX IF NOT EXISTS idx_staging_articles_author_id ON staging_articles(author_id);
CREATE INDEX IF NOT EXISTS idx_staging_articles_is_valid ON staging_articles(job_id, is_valid);

-- Staging table for comments
CREATE TABLE IF NOT EXISTS staging_comments (
    staging_id SERIAL PRIMARY KEY,
    job_id UUID NOT NULL REFERENCES jobs(id) ON DELETE CASCADE,
    row_number INTEGER NOT NULL,
    id VARCHAR(255),
    article_id VARCHAR(255),
    user_id VARCHAR(255),
    body TEXT,
    created_at VARCHAR(255),
    validation_error VARCHAR(255),
    is_valid BOOLEAN DEFAULT true,
    is_duplicate BOOLEAN DEFAULT false,
    processed BOOLEAN DEFAULT false
);

-- Create indexes for staging_comments
CREATE INDEX IF NOT EXISTS idx_staging_comments_job_id ON staging_comments(job_id);
CREATE INDEX IF NOT EXISTS idx_staging_comments_article_id ON staging_comments(article_id);
CREATE INDEX IF NOT EXISTS idx_staging_comments_user_id ON staging_comments(user_id);
CREATE INDEX IF NOT EXISTS idx_staging_comments_is_valid ON staging_comments(job_id, is_valid);

-- ============================================
-- Functions and Triggers
-- ============================================

-- Function to update updated_at timestamp
CREATE OR REPLACE FUNCTION update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$ language 'plpgsql';

-- Create triggers for updated_at
CREATE TRIGGER update_users_updated_at BEFORE UPDATE ON users
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER update_articles_updated_at BEFORE UPDATE ON articles
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER update_comments_updated_at BEFORE UPDATE ON comments
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER update_jobs_updated_at BEFORE UPDATE ON jobs
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

-- ============================================
-- Cleanup function for expired idempotency keys
-- ============================================
CREATE OR REPLACE FUNCTION cleanup_expired_idempotency_keys()
RETURNS void AS $$
BEGIN
    DELETE FROM idempotency_keys WHERE expires_at < NOW();
END;
$$ LANGUAGE plpgsql;
