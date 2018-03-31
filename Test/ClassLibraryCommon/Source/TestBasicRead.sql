SELECT TOP 100 
	TransactionID rk, 
	ModifiedDate w, 
	Quantity [p.Quantity] 
FROM 
	AdventureWorks2017.Production.TransactionHistory