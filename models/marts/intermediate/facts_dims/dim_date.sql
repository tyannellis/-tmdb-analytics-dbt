with dates as (

    select *
    from (
        {{ dbt_utils.date_spine(
            start_date="'2018-01-01'",
            end_date="'2030-12-31'",
            datepart="day"
        ) }}
    ) as spine

),

final as (

    select
        date_day,
        extract(year from date_day) as year,
        extract(month from date_day) as month,
        monthname(date_day) as month_name,                 
        extract(quarter from date_day) as quarter,
        date_trunc('week', date_day) as week_start_date,
        to_char(date_day, 'DY') as day_of_week,
        dayofweek(date_day) as day_number_of_week,
        case when dayofweek(date_day) in (1, 7) then true else false end as is_weekend
    from dates

)

select * from final
