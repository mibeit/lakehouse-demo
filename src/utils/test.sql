CREATE TABLE CITIES
(
CityID int,
CityName char(50),
StateProvinceID int,
Location char(100),
LatestRecordedPopulation float,
LastEditedBy int,
ValidFrom char(20),
ValidTo char(20),
PRIMARY KEY (CityID)
);

CREATE TABLE COLORS
(
ColorID int,
ColorName char(50),
LastEditedBy int,
ValidFrom char(20),
ValidTo char(20),
PRIMARY KEY (ColorID)
);

CREATE TABLE COUNTRIES
(
CountryID int,
CountryName char(100),
FormalName char(100),
IsoAlpha3Code char(3),
IsoNumericCode int,
CountryType char(50),
LatestRecordedPopulation int,
Continent char(50),
Region char(50),
Subregion char(50),
Border char(20),
LastEditedBy int,
ValidFrom char(20),
ValidTo char(20),
PRIMARY KEY (CountryID)
);

CREATE TABLE CUSTOMER
(
CustomerID int,
CustomerName char(100),
BillToCustomerID int,
CustomerCategoryID int,
BuyingGroupID int,
PrimaryContactPersonID int,
AlternateContactPersonID int,
DeliveryMethodID int,
DeliveryCityID int,
PostalCityID int,
CreditLimit float,
AccountOpenedDate char(20),
StandardDiscountPercentage float,
IsStatementSent int,
IsOnCreditHold int,
PaymentDays int,
PhoneNumber char(20),
FaxNumber char(20),
DeliveryRun float,
RunPosition float,
WebsiteURL char(100),
DeliveryAddressLine1 char(100),
DeliveryAddressLine2 char(100),
DeliveryPostalCode int,
DeliveryLocation char(100),
PostalAddressLine1 char(100),
PostalAddressLine2 char(100),
PostalPostalCode int,
LastEditedBy int,
ValidFrom char(20),
ValidTo char(20),
ValidFrom_parsed char(20),
PRIMARY KEY (CustomerID)
);

CREATE TABLE DELIVERYMETHODS
(
DeliveryMethodID int,
DeliveryMethodName char(50),
LastEditedBy int,
ValidFrom char(20),
ValidTo char(20),
PRIMARY KEY (DeliveryMethodID)
);

CREATE TABLE INVOICES
(
InvoiceID int,
CustomerID int,
BillToCustomerID int,
OrderID int,
DeliveryMethodID int,
ContactPersonID int,
AccountsPersonID int,
SalespersonPersonID int,
PackedByPersonID int,
InvoiceDate char(20),
CustomerPurchaseOrderNumber int,
IsCreditNote int,
CreditNoteReason float,
Comments float,
DeliveryInstructions char(255),
InternalComments float,
TotalDryItems int,
TotalChillerItems int,
DeliveryRun float,
RunPosition float,
ReturnedDeliveryData char(255),
ConfirmedDeliveryTime char(20),
ConfirmedReceivedBy char(100),
LastEditedBy int,
LastEditedWhen char(20),
LastEditedWhen_parsed char(20),
PRIMARY KEY (InvoiceID)
);

CREATE TABLE INVOICESLINES
(
InvoiceLineID int,
InvoiceID int,
StockItemID int,
Description char(255),
PackageTypeID int,
Quantity int,
UnitPrice float,
TaxRate float,
TaxAmount float,
LineProfit float,
ExtendedPrice float,
LastEditedBy int,
LastEditedWhen char(20),
LastEditedWhen_parsed char(20),
PRIMARY KEY (InvoiceLineID)
);

CREATE TABLE PACKAGETYPES
(
PackageTypeID int,
PackageTypeName char(50),
LastEditedBy int,
ValidFrom char(20),
ValidTo char(20),
PRIMARY KEY (PackageTypeID)
);

CREATE TABLE PAYMENTMETHOD
(
PaymentMethodID int,
PaymentMethodName char(50),
LastEditedBy int,
ValidFrom char(20),
ValidTo char(20),
PRIMARY KEY (PaymentMethodID)
);

CREATE TABLE PEOPLE
(
PersonID int,
FullName char(100),
PreferredName char(50),
SearchName char(100),
IsPermittedToLogon int,
LogonName char(50),
IsExternalLogonProvider int,
HashedPassword float,
IsSystemUser int,
IsEmployee int,
IsSalesperson int,
UserPreferences float,
PhoneNumber char(20),
FaxNumber char(20),
EmailAddress char(100),
Photo float,
CustomFields float,
OtherLanguages float,
LastEditedBy int,
ValidFrom char(20),
ValidTo char(20),
ValidFrom_parsed char(20),
PRIMARY KEY (PersonID)
);

CREATE TABLE PROVINCE
(
StateProvinceID int,
StateProvinceCode char(10),
StateProvinceName char(50),
CountryID int,
SalesTerritory char(50),
Border char(20),
LatestRecordedPopulation int,
LastEditedBy int,
ValidFrom char(20),
ValidTo char(20),
PRIMARY KEY (StateProvinceID)
);

CREATE TABLE PURCHASE_ORDER
(
PurchaseOrderID int,
SupplierID int,
OrderDate char(20),
DeliveryMethodID int,
ContactPersonID int,
ExpectedDeliveryDate char(20),
SupplierReference char(50),
IsOrderFinalized int,
Comments float,
InternalComments float,
LastEditedBy int,
LastEditedWhen char(20),
LastEditedWhen_parsed char(20),
PRIMARY KEY (PurchaseOrderID)
);

CREATE TABLE PURCHASE_ORDERLINE
(
PurchaseOrderLineID int,
PurchaseOrderID int,
StockItemID int,
OrderedOuters int,
Description char(255),
ReceivedOuters int,
PackageTypeID int,
ExpectedUnitPricePerOuter float,
LastReceiptDate char(20),
IsOrderLineFinalized int,
LastEditedBy int,
LastEditedWhen char(20),
LastEditedWhen_parsed char(20),
PRIMARY KEY (PurchaseOrderLineID)
);

CREATE TABLE SALES_ORDER
(
OrderID int,
CustomerID int,
SalespersonPersonID int,
PickedByPersonID float,
ContactPersonID int,
BackorderOrderID float,
OrderDate char(20),
ExpectedDeliveryDate char(20),
CustomerPurchaseOrderNumber int,
IsUndersupplyBackordered int,
Comments float,
DeliveryInstructions float,
InternalComments float,
PickingCompletedWhen char(20),
LastEditedBy int,
LastEditedWhen char(20),
LastEditedWhen_parsed char(20),
PRIMARY KEY (OrderID)
);

CREATE TABLE SALES_ORDERLINE
(
OrderLineID int,
OrderID int,
StockItemID int,
Description char(255),
PackageTypeID int,
Quantity int,
UnitPrice float,
TaxRate float,
PickedQuantity int,
PickingCompletedWhen char(20),
LastEditedBy int,
LastEditedWhen char(20),
LastEditedWhen_parsed char(20),
PRIMARY KEY (OrderLineID)
);

CREATE TABLE STOCKGROUPS
(
StockGroupID int,
StockGroupName char(50),
LastEditedBy int,
ValidFrom char(20),
ValidTo char(20),
PRIMARY KEY (StockGroupID)
);

CREATE TABLE STOCKITEMHOLDINGS
(
StockItemID int,
QuantityOnHand int,
BinLocation char(20),
LastStocktakeQuantity int,
LastCostPrice float,
ReorderLevel int,
TargetStockLevel int,
LastEditedBy int,
LastEditedWhen char(20),
PRIMARY KEY (StockItemID)
);

CREATE TABLE STOCKITEMS
(
StockItemID int,
StockItemName char(100),
SupplierID int,
ColorID float,
UnitPackageID int,
OuterPackageID int,
Brand char(50),
Size char(20),
LeadTimeDays int,
QuantityPerOuter int,
IsChillerStock int,
Barcode float,
TaxRate float,
UnitPrice float,
RecommendedRetailPrice float,
TypicalWeightPerUnit float,
MarketingComments char(255),
InternalComments float,
Photo float,
CustomFields char(255),
Tags char(255),
SearchDetails char(255),
LastEditedBy int,
ValidFrom char(20),
ValidTo char(20),
PRIMARY KEY (StockItemID)
);

CREATE TABLE SUPPLIERS
(
SupplierID int,
SupplierName char(100),
SupplierCategoryID int,
PrimaryContactPersonID int,
AlternateContactPersonID int,
DeliveryMethodID float,
DeliveryCityID int,
PostalCityID int,
SupplierReference char(50),
BankAccountName char(100),
BankAccountBranch char(100),
BankAccountCode int,
BankAccountNumber int,
BankInternationalCode int,
PaymentDays int,
InternalComments char(255),
PhoneNumber char(20),
FaxNumber char(20),
WebsiteURL char(100),
DeliveryAddressLine1 char(100),
DeliveryAddressLine2 char(100),
DeliveryPostalCode int,
DeliveryLocation char(100),
PostalAddressLine1 char(100),
PostalAddressLine2 char(100),
PostalPostalCode int,
LastEditedBy int,
ValidFrom char(20),
ValidTo char(20),
PRIMARY KEY (SupplierID)
);

CREATE TABLE SUPPLIERSTRANSACTIONS
(
SupplierTransactionID int,
SupplierID int,
TransactionTypeID int,
PurchaseOrderID float,
PaymentMethodID int,
SupplierInvoiceNumber float,
TransactionDate char(20),
AmountExcludingTax float,
TaxAmount float,
TransactionAmount float,
OutstandingBalance float,
FinalizationDate char(20),
IsFinalized int,
LastEditedBy int,
LastEditedWhen char(20),
PRIMARY KEY (SupplierTransactionID)
);

CREATE TABLE TRANSACTIONTYPES
(
TransactionTypeID int,
TransactionTypeName char(50),
LastEditedBy int,
ValidFrom char(20),
ValidTo char(20),
PRIMARY KEY (TransactionTypeID)
);
