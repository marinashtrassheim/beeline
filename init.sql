-- Создание схемы ods
CREATE SCHEMA IF NOT EXISTS ods;

-- Таблица для показов баннеров
CREATE TABLE IF NOT EXISTS ods.fct_banners (
    banner_id VARCHAR(50),
    campaign_id VARCHAR(50),
    user_id VARCHAR(50),
    event_timestamp TIMESTAMP,
    placement VARCHAR(100),
    device_type VARCHAR(50),
    os VARCHAR(50),
    geo VARCHAR(50),
    is_clicked INTEGER,
    processing_date DATE DEFAULT CURRENT_DATE
);

-- Таблица для установок
CREATE TABLE IF NOT EXISTS ods.installs (
    user_id VARCHAR(50),
    install_timestamp TIMESTAMP,
    source VARCHAR(100),
    processing_date DATE DEFAULT CURRENT_DATE
);

-- Таблица для действий пользователей
CREATE TABLE IF NOT EXISTS ods.fct_actions (
    user_id VARCHAR(50),
    session_start TIMESTAMP,
    actions TEXT,
    processing_date DATE DEFAULT CURRENT_DATE
);

-- Таблица для метаданных баннеров
CREATE TABLE IF NOT EXISTS ods.cd_banner (
    banner_id VARCHAR(50) PRIMARY KEY,
    creative_type VARCHAR(50),
    message TEXT,
    size VARCHAR(50),
    target_audience_segment VARCHAR(100),
    processing_date DATE DEFAULT CURRENT_DATE
);

-- Таблица для кампаний
CREATE TABLE IF NOT EXISTS ods.cd_campaign (
    campaign_id VARCHAR(50) PRIMARY KEY,
    daily_budget DECIMAL(15,2),
    start_date DATE,
    end_date DATE,
    impressions BIGINT,
    clicks BIGINT,
    calculated_cpm DECIMAL(15,2),
    calculated_cpc DECIMAL(15,2),
    processing_date DATE DEFAULT CURRENT_DATE
);

-- Таблица для пользователей
CREATE TABLE IF NOT EXISTS ods.cd_user (
    user_id VARCHAR(50) PRIMARY KEY,
    segment VARCHAR(50),
    tariff VARCHAR(50),
    date_create DATE,
    date_end DATE,
    processing_date DATE DEFAULT CURRENT_DATE
);

-- Создание индексов для ускорения запросов
CREATE INDEX IF NOT EXISTS idx_fct_banners_user_id ON ods.fct_banners(user_id);
CREATE INDEX IF NOT EXISTS idx_fct_banners_campaign_id ON ods.fct_banners(campaign_id);
CREATE INDEX IF NOT EXISTS idx_installs_user_id ON ods.installs(user_id);
CREATE INDEX IF NOT EXISTS idx_fct_actions_user_id ON ods.fct_actions(user_id);
CREATE INDEX IF NOT EXISTS idx_fct_banners_timestamp ON ods.fct_banners(event_timestamp);
CREATE INDEX IF NOT EXISTS idx_installs_timestamp ON ods.installs(install_timestamp);
CREATE INDEX IF NOT EXISTS idx_fct_actions_session_start ON ods.fct_actions(session_start);