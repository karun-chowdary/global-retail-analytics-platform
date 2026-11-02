-- Run connected as RETAIL_SRC (service FREEPDB1).
-- Oracle holds the transactional tables: orders, order_items, order_payments.
-- last_modified is the WATERMARK column for incremental loads in Fabric pipelines.

BEGIN
  FOR t IN (SELECT table_name FROM user_tables
            WHERE table_name IN ('ORDER_PAYMENTS', 'ORDER_ITEMS', 'ORDERS')) LOOP
    EXECUTE IMMEDIATE 'DROP TABLE ' || t.table_name || ' PURGE';
  END LOOP;
END;
/

CREATE TABLE orders (
  order_id                       VARCHAR2(32)  NOT NULL,
  customer_id                    VARCHAR2(32)  NOT NULL,
  order_status                   VARCHAR2(20)  NOT NULL,
  order_purchase_timestamp       TIMESTAMP,
  order_approved_at              TIMESTAMP,
  order_delivered_carrier_date   TIMESTAMP,
  order_delivered_customer_date  TIMESTAMP,
  order_estimated_delivery_date  TIMESTAMP,
  last_modified                  TIMESTAMP DEFAULT SYSTIMESTAMP NOT NULL,
  CONSTRAINT pk_orders PRIMARY KEY (order_id)
);

CREATE TABLE order_items (
  order_id             VARCHAR2(32) NOT NULL,
  order_item_id        NUMBER(4)    NOT NULL,
  product_id           VARCHAR2(32) NOT NULL,
  seller_id            VARCHAR2(32) NOT NULL,
  shipping_limit_date  TIMESTAMP,
  price                NUMBER(12,2),
  freight_value        NUMBER(12,2),
  last_modified        TIMESTAMP DEFAULT SYSTIMESTAMP NOT NULL,
  CONSTRAINT pk_order_items PRIMARY KEY (order_id, order_item_id)
);

CREATE TABLE order_payments (
  order_id              VARCHAR2(32) NOT NULL,
  payment_sequential    NUMBER(4)    NOT NULL,
  payment_type          VARCHAR2(20),
  payment_installments  NUMBER(4),
  payment_value         NUMBER(12,2),
  last_modified         TIMESTAMP DEFAULT SYSTIMESTAMP NOT NULL,
  CONSTRAINT pk_order_payments PRIMARY KEY (order_id, payment_sequential)
);

CREATE INDEX ix_orders_last_modified    ON orders (last_modified);
CREATE INDEX ix_items_last_modified     ON order_items (last_modified);
CREATE INDEX ix_payments_last_modified  ON order_payments (last_modified);
CREATE INDEX ix_orders_purchase_ts      ON orders (order_purchase_timestamp);
