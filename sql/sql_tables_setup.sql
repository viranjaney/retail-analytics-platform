IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = 'sales')
    EXEC('CREATE SCHEMA sales');
GO

CREATE TABLE sales.customers (
    customer_id               CHAR(32)      NOT NULL,
    customer_unique_id        CHAR(32)      NOT NULL,
    customer_zip_code_prefix  CHAR(5)       NOT NULL,
    customer_city             NVARCHAR(100) NOT NULL,
    customer_state            CHAR(2)       NOT NULL,
    CONSTRAINT pk_customers PRIMARY KEY (customer_id)
);

CREATE TABLE sales.sellers (
    seller_id                 CHAR(32)      NOT NULL,
    seller_zip_code_prefix    CHAR(5)       NOT NULL,
    seller_city               NVARCHAR(100) NOT NULL,
    seller_state              CHAR(2)       NOT NULL,
    CONSTRAINT pk_sellers PRIMARY KEY (seller_id)
);

CREATE TABLE sales.product_category_translation (
    product_category_name          VARCHAR(100) NOT NULL,
    product_category_name_english  VARCHAR(100) NOT NULL,
    CONSTRAINT pk_category_translation PRIMARY KEY (product_category_name)
);

CREATE TABLE sales.products (
    product_id                   CHAR(32)     NOT NULL,
    product_category_name        VARCHAR(100) NULL,
    product_name_lenght          INT          NULL,   -- misspelling is from the source CSV
    product_description_lenght   INT          NULL,
    product_photos_qty           INT          NULL,
    product_weight_g             INT          NULL,
    product_length_cm            INT          NULL,
    product_height_cm            INT          NULL,
    product_width_cm             INT          NULL,
    CONSTRAINT pk_products PRIMARY KEY (product_id)
);

CREATE TABLE sales.orders (
    order_id                       CHAR(32)     NOT NULL,
    customer_id                    CHAR(32)     NOT NULL,
    order_status                   VARCHAR(20)  NOT NULL,
    order_purchase_timestamp       DATETIME2(0) NOT NULL,
    order_approved_at              DATETIME2(0) NULL,
    order_delivered_carrier_date   DATETIME2(0) NULL,
    order_delivered_customer_date  DATETIME2(0) NULL,
    order_estimated_delivery_date  DATETIME2(0) NOT NULL,
    CONSTRAINT pk_orders PRIMARY KEY (order_id),
    CONSTRAINT fk_orders_customers FOREIGN KEY (customer_id)
        REFERENCES sales.customers (customer_id)
);

CREATE TABLE sales.order_items (
    order_id             CHAR(32)      NOT NULL,
    order_item_id        INT           NOT NULL,   -- line number within the order
    product_id           CHAR(32)      NOT NULL,
    seller_id            CHAR(32)      NOT NULL,
    shipping_limit_date  DATETIME2(0)  NOT NULL,
    price                DECIMAL(10,2) NOT NULL,
    freight_value        DECIMAL(10,2) NOT NULL,
    CONSTRAINT pk_order_items PRIMARY KEY (order_id, order_item_id),
    CONSTRAINT fk_items_orders   FOREIGN KEY (order_id)   REFERENCES sales.orders (order_id),
    CONSTRAINT fk_items_products FOREIGN KEY (product_id) REFERENCES sales.products (product_id),
    CONSTRAINT fk_items_sellers  FOREIGN KEY (seller_id)  REFERENCES sales.sellers (seller_id)
);

CREATE TABLE sales.order_payments (
    order_id              CHAR(32)      NOT NULL,
    payment_sequential    INT           NOT NULL,
    payment_type          VARCHAR(20)   NOT NULL,
    payment_installments  INT           NOT NULL,
    payment_value         DECIMAL(10,2) NOT NULL,
    CONSTRAINT pk_order_payments PRIMARY KEY (order_id, payment_sequential),
    CONSTRAINT fk_payments_orders FOREIGN KEY (order_id) REFERENCES sales.orders (order_id)
);
GO

CREATE TABLE sales.geolocation (
    geolocation_zip_code_prefix  CHAR(5)       NOT NULL,
    geolocation_lat              DECIMAL(9,6)  NOT NULL,
    geolocation_lng              DECIMAL(9,6)  NOT NULL,
    geolocation_city             NVARCHAR(100) NOT NULL,
    geolocation_state            CHAR(2)       NOT NULL
);
CREATE CLUSTERED INDEX ix_geolocation_zip
    ON sales.geolocation (geolocation_zip_code_prefix);

CREATE TABLE sales.order_reviews (
    review_pk                 INT IDENTITY(1,1) NOT NULL,
    review_id                 CHAR(32)          NOT NULL,
    order_id                  CHAR(32)          NOT NULL,
    review_score              TINYINT           NOT NULL,
    review_comment_title      NVARCHAR(100)     NULL,
    review_comment_message    NVARCHAR(500)     NULL,
    review_creation_date      DATETIME2(0)      NOT NULL,
    review_answer_timestamp   DATETIME2(0)      NOT NULL,
    CONSTRAINT pk_order_reviews PRIMARY KEY (review_pk),
    CONSTRAINT fk_reviews_orders FOREIGN KEY (order_id) REFERENCES sales.orders (order_id),
    CONSTRAINT ck_review_score CHECK (review_score BETWEEN 1 AND 5)
);
GO