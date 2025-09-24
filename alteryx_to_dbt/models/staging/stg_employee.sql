with
    raw as (
        select * 
        from {{ ref('employee') }}
    )

    , renamed as (
        select
            cast(BusinessEntityID as int) as business_entity_id
            , cast(NationalIDNumber as int) as national_id_number
            , LoginID as login_id
            , JobTitle as job_title
            , BirthDate as birth_date
            , case 
                when MaritalStatus = 'S' then 'Single'
                when MaritalStatus = 'M' then 'Married'
                else '0'
            end as marital_status
            , case
                when Gender = 'M' then 'Male'
                when Gender = 'F' then 'Female'
                else '0'
            end as gender
            , HireDate as hire_date
            , SalariedFlag as salaried_flag
            , VacationHours as vacation_hours
            , SickLeaveHours as sick_leave_hours
            , CurrentFlag as current_flag
            , rowguid
            , ModifiedDate as updated_at
            , extraction_date as extracted_at
            , OrganizationNode as organization_node
            , OrganizationLevel as organization_level
        from raw    
    )

select *
from renamed
