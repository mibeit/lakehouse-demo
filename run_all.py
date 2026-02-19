from src.etl.sales.order_transformer import OrderTransformer
from src.etl.sales.orderline_transformer import OrderLineTransformer
from src.etl.sales.customer_transformer import CustomerTransformer
from src.etl.sales.invoice_transformer import InvoiceTransformer
from src.etl.sales.invoice_line_transformer import InvoiceLineTransformer

from src.etl.purchasing.purchase_order_transformer import PurchaseOrderTransformer
from src.etl.purchasing.purchase_order_line_transformer import PurchaseOrderLineTransformer
from src.etl.purchasing.supplier_transformer import SupplierTransformer
from src.etl.purchasing.supplier_transaction_transformer import SupplierTransactionTransformer

from src.etl.dimensions.dimension_transformer import DimensionTransformer, DIMENSIONS_CONFIG
from src.etl.dimensions.cities_transformer import CitiesTransformer
from src.etl.dimensions.province_transformer import ProvinceTransformer
from src.etl.dimensions.people_transformer import PeopleTransformer
from src.etl.dimensions.stock_item_transformer import StockItemTransformer
from src.etl.dimensions.stock_item_holdings_transformer import StockItemHoldingsTransformer


def run_all():
    transformers = [
        OrderTransformer(),
        OrderLineTransformer(),
        CustomerTransformer(),
        InvoiceTransformer(),
        InvoiceLineTransformer(),

        PurchaseOrderTransformer(),
        PurchaseOrderLineTransformer(),
        SupplierTransformer(),
        SupplierTransactionTransformer(),

        *[DimensionTransformer(name) for name in DIMENSIONS_CONFIG],


        CitiesTransformer(),
        ProvinceTransformer(),
        PeopleTransformer(),
        StockItemTransformer(),
        StockItemHoldingsTransformer(),
    ]

    for transformer in transformers:
        transformer.run()

    print(f"\n✅ All {len(transformers)} Transformer successfully completed!")


if __name__ == "__main__":
    run_all()
