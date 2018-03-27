SELECT TOP 100 
	TransactionID rk, 
	ModifiedDate w, 
	Quantity [p.Quantity] 
FROM 
	AdventureWorks2017.Production.TransactionHistory
WHERE
	ModifiedDate > '{Watermark, w, 2000-01-01}'