-- BASIC STORED PROCEDURE
DROP PROCEDURE IF EXISTS GetAllProducts;
-- Once Created, procedures cant alter -> delete


DELIMITER // 

CREATE PROCEDURE GetAllProducts()
BEGIN
	SELECT *  FROM products;
END //

DELIMITER ;

-- Run it
call GetAllProducts();

DROP PROCEDURE IF EXISTS GetOfficeByCountry;

DELIMITER //

CREATE PROCEDURE GetOfficeByCountry(
	IN countryName VARCHAR(255)
)
BEGIN
	SELECT * 
 		FROM offices
			WHERE country = countryName;
END //
DELIMITER ;

-- Execute Function: CALL
call GetOfficeByCountry('USA');
call GetOfficeByCountry('France');
call GetOfficeByCountry();


-- Exercise 1) Create stored procedure - display 1st x payment table entries. 
-- X = IN parameter 

DELIMITER //

CREATE PROCEDURE Get_X_Payments(
	IN x Int
)
BEGIN
	SELECT * FROM payments
			limit x ;
END //
DELIMITER ;

CALL Get_x_payments(15);

DROP PROCEDURE IF EXISTS GetOrderCountByStatus;

DELIMITER $$

CREATE PROCEDURE GetOrderCountByStatus (
	IN  orderStatus VARCHAR(25),
	OUT total INT
)
BEGIN
	SELECT COUNT(orderNumber)
	INTO total
	FROM orders
	WHERE status = orderStatus;
END$$
DELIMITER ;

CALL GetOrderCountByStatus('Shipped',@total);
SELECT @total;


-- Exercise 2) Stored procedure -> return amount 4 Xth entry of payment table. 
Drop procedure if exists Get_Xth_Payment;

DELIMITER //

CREATE PROCEDURE Get_Xth_Payment(
	IN Value1 Int,
    out AmountVal decimal(10,2)
)
BEGIN
	set Value1 = Value1-1;
    SELECT amount 
		into AmountVal
            FROM payments
            limit Value1,1;
END //
DELIMITER ;

call Get_Xth_Payment(10, @AmountVal);
select @AmountVal;

-- Exercise 3) Create stored procedure -> return category of given row.
-- Row Nr. = IN parameter, Category = OUT parameter
-- CAT1 > 100 000 >= CAT2 > 10000 >= CAT3

DROP PROCEDURE IF EXISTS GetRowCateg;

DELIMITER $$

CREATE PROCEDURE GetRowCateg(
    	IN  RowNr INT,
    	OUT Category  CHAR(4)
)
BEGIN
  	SET RowNr = RowNr-1;
	SELECT * FROM payments limit RowNr, 1;

	IF RowNr > 100000 THEN
		SET Category = 'CAT1';
	ELSEIF RowNr > 10000 THEN
		SET Category = 'CAT2';
	ELSE 
		SET Category = 'CAT3';
	END IF;
END$$
DELIMITER ;

call GetRowCateg(10, @Category);
select @Category;

use classicmodels;
SELECT count(*)  FROM customers;
SELECT * FROM payments limit 12, 1;

