with
    raw as (
        select * 
        from {{ ref('product') }}
    )

    , renamed as (
        select
            cast(ProductID as int) as product_id
            , Name as product_name
            , ProductNumber as product_number
            , cast(MakeFlag as boolean) as make_flag
            , cast(FinishedGoodsFlag as boolean) as finished_goods_flag
            , cast(Color as string) as color
            , cast(SafetyStockLevel as int) as safety_stock_level
            , cast(ReorderPoint as int) as re_order_point
            , cast(StandardCost as float) as standard_cost
            , cast(ListPrice as float) as list_price
            , Size as product_size
            , SizeUnitMeasureCode as size_unit_measure_code
            , WeightUnitMeasureCode as weight_unit_measure_code
            , cast(Weight as float) as product_weight
            , cast(DaysToManufacture as int) as days_to_manufacture
            , ProductLine as product_line
            , Class as product_class
            , Style as product_style
            , cast(ProductSubcategoryID as int) as product_subcategory_id
            , cast(ProductModelID as int) as product_model_id
            , SellStartDate as sell_start_date
            , cast(SellEndDate as date) as sell_end_date
            , cast(DiscontinuedDate as timestamp) as discontinued_date
            , rowguid
            , cast(ModifiedDate as date) as updated_at
            , cast(extraction_date as timestamp) as extracted_at
        from raw    
    )

select *
from renamed
