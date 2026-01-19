UPDATE myDataset.Inventory T
SET quantity = n.quantity
FROM (
select quantity, product from myDataset.NewArrivals
)n
WHERE i.product = n.product