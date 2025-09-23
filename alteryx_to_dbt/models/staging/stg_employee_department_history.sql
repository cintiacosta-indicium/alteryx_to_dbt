with
    raw as (
        select * 
        from {{ ref('employeedepartmenthistory') }}
    )

    , renamed as (
        select
            cast(BusinessEntityID as int) as business_entity_id
            , cast(DepartmentID as int) as department_id
            , cast(ShiftID as int) as shift_id
            , cast(StartDate as date) as work_start_date
            , cast(EndDate as date) as work_end_date
            , cast(ModifiedDate as date) as updated_at
            , cast(extraction_date as timestamp) as extracted_at
        from raw    
    )

select *
from renamed
