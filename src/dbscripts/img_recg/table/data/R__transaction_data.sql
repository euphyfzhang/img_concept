CREATE OR REPLACE TABLE TRANSACTION(_ID INTEGER AUTOINCREMENT ORDER, TRANSACTION_TIMESTAMP TIMESTAMP, MERCHANT_ID NUMBER, MERCHANT_NAME VARCHAR,
PRODUCT_ID NUMBER, ITEM VARCHAR, AMOUNT NUMBER(38,2));

INSERT OVERWRITE INTO TRANSACTION(TRANSACTION_TIMESTAMP, MERCHANT_ID, MERCHANT_NAME, PRODUCT_ID, ITEM, AMOUNT) 
VALUES 
('2025-03-15 09:23:45', 1, 'Loblaws Store #1234', 1, 'Granola bar', 3.99),
('2025-03-16 11:45:12', 2, 'Metro Store #9876', 2, 'Oatmeal', 4.49),
('2025-03-17 14:30:22', 3, 'Costco Store #4567', 3, 'Goldfish cracker', 8.99),
('2025-03-18 16:18:37', 4, 'Walmart Store #7890', 4, 'Pez candy', 1.99),
('2025-03-19 08:12:53', 1, 'Loblaws Store #1234', 2, 'Oatmeal', 4.29),
('2025-03-20 10:05:41', 2, 'Metro Store #9876', 1, 'Granola bar', 3.79),
('2025-03-21 12:47:19', 3, 'Costco Store #4567', 4, 'Pez candy', 5.99),
('2025-03-22 15:33:28', 4, 'Walmart Store #7890', 3, 'Goldfish cracker', 3.49),
('2025-03-23 07:58:14', 1, 'Loblaws Store #1234', 3, 'Goldfish cracker', 4.99),
('2025-03-24 09:42:36', 2, 'Metro Store #9876', 4, 'Pez candy', 2.29),
('2025-03-25 13:15:47', 3, 'Costco Store #4567', 1, 'Granola bar', 7.49),
('2025-03-26 17:22:09', 4, 'Walmart Store #7890', 2, 'Oatmeal', 3.99),
('2025-03-28 08:45:33', 1, 'Loblaws Store #1234',4,  'Pez candy', 2.49),
('2025-03-28 11:30:18', 2, 'Metro Store #9876', 3, 'Goldfish cracker', 3.99),
('2025-03-28 14:12:55', 3, 'Costco Store #4567', 2, 'Oatmeal', 6.99),
('2025-03-28 16:45:27', 4, 'Walmart Store #7890', 1, 'Granola bar', 3.29),
('2025-03-29 10:15:42', 1, 'Loblaws Store #1234', 1, 'Granola bar', 3.99),
('2025-03-29 12:30:11', 2, 'Metro Store #9876', 2, 'Oatmeal', 4.19),
('2025-03-29 15:45:39', 3, 'Costco Store #4567', 3, 'Goldfish cracker', 9.49),
('2025-03-29 18:20:05', 4, 'Walmart Store #7890', 4, 'Pez candy', 1.79);


CREATE OR REPLACE TABLE IMG_RECG.DIM_MERCHANT(
    _LOAD_TS TIMESTAMP_NTZ NOT NULL DEFAULT SYSDATE()
    , _ID NUMBER NOT NULL
    , MERCHANT_NAME VARCHAR
    );
INSERT OVERWRITE INTO IMG_RECG.DIM_MERCHANT(_ID, MERCHANT_NAME)
VALUES (1, 'Loblaws Store #1234')
, (2, 'Metro Store #9876')
, (3, 'Costco Store #4567')
, (4, 'Walmart Store #7890');

CREATE OR REPLACE TABLE IMG_RECG.DIM_PRODUCT(
    _LOAD_TS TIMESTAMP_NTZ NOT NULL DEFAULT SYSDATE()
    , _ID NUMBER NOT NULL
    , PRODUCT_NAME VARCHAR
    );

INSERT OVERWRITE INTO IMG_RECG.DIM_PRODUCT(_ID, PRODUCT_NAME)
VALUES (1, 'Granola bar')
, (2, 'Oatmeal')
, (3, 'Goldfish cracker')
, (4, 'Pez candy');