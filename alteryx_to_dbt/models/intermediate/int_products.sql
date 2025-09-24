with 
    product as (
        select 
            product_id
            , product_name
            , product_number
            , make_flag
            , finished_goods_flag
            , color
            , safety_stock_level
            , re_order_point
            , standard_cost
            , list_price
            , product_size
            , size_unit_measure_code
            , weight_unit_measure_code
            , product_weight
            , days_to_manufacture
            , product_line
            , product_class
            , product_style
            , product_subcategory_id
            , product_model_id
            , sell_start_date
            , sell_end_date
            , discontinued_date
            , rowguid
            , updated_at
            , extracted_at
        from {{ ref("stg_product") }}
    )

    , product_subcategory as (
        select
            product_subcategory_id
            , product_category_id
            , subcategory_description
            , rowguid
            , updated_at
            , extracted_at
        from {{ ref("stg_product_subcategory") }}
    )

    , product_category as (
        select
            product_category_id
            , category_description
            , rowguid
            , updated_at
            , extracted_at
        from {{ ref("stg_product_category") }}
    )

    , joined_products as (
        select 
            {{ dbt_utils.default__generate_surrogate_key(
                ['product.product_id']
            ) }} as product_sk
            , product.product_id
            , product.product_name
            , product.product_number
            , product.make_flag
            , product.finished_goods_flag
            , product.color
            , product.safety_stock_level
            , product.re_order_point
            , product.standard_cost
            , product.list_price
            , product.product_size
            , product.size_unit_measure_code
            , product.weight_unit_measure_code
            , product.product_weight
            , product.days_to_manufacture
            , product.product_line
            , product.product_class
            , product.product_style
            , product.product_subcategory_id
            , product.product_model_id
            , product.sell_start_date
            , product.sell_end_date
            , product.discontinued_date
            , product.rowguid
            , product.updated_at
            , product.extracted_at
            , product_subcategory.product_category_id
            , product_subcategory.subcategory_description
            , product_category.category_description
        from product
        left join product_subcategory on
            product.product_subcategory_id = product_subcategory.product_subcategory_id
        left join product_category on
            product_subcategory.product_category_id = product_category.product_category_id
        
    )

select *
from joined_products
