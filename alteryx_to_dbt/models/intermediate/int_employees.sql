with
    employee as (
        select
            business_entity_id
            , national_id_number
            , login_id
            , job_title
            , birth_date
            , marital_status
            , gender
            , hire_date
            , salaried_flag
            , vacation_hours
            , sick_leave_hours
            , current_flag
            , rowguid
            , updated_at
            , extracted_at
            , organization_node
            , organization_level
        from {{ ref("stg_employee") }}
    )

    , employee_department_history as (
        select
            business_entity_id
            , department_id
            , shift_id
            , work_start_date
            , work_end_date
            , updated_at
            , extracted_at
        from {{ ref("stg_employee_department_history") }}
    )

    , joined_employee as (
        select 
            {{ dbt_utils.default__generate_surrogate_key(
                ['employee.business_entity_id']
            ) }} as employee_sk
            , employee.business_entity_id
            , employee.national_id_number
            , employee.login_id
            , employee.job_title
            , employee.birth_date
            , employee.marital_status
            , employee.gender
            , employee.hire_date
            , employee.salaried_flag
            , employee.vacation_hours
            , employee.sick_leave_hours
            , employee.current_flag
            , employee.rowguid
            , employee.updated_at
            , employee.extracted_at
            , employee.organization_node
            , employee.organization_level
            , employee_department_history.department_id
            , employee_department_history.shift_id
            , employee_department_history.work_start_date
            , employee_department_history.work_end_date
        from employee
        left join employee_department_history on
            employee.business_entity_id = employee_department_history.business_entity_id
    )
select *
from joined_employee
