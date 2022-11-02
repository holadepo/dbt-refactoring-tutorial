with 

    -- Import CTEs
    orders as (
        select 
            * 
        from 
            {{ source('jaffle_shop', 'orders') }}
    ),

    customers as (
        select 
            *
        from 
            {{ source('jaffle_shop', 'customers') }}
    ),

    payments as (
        select 
            *
        from
            {{ source('stripe', 'payment') }}
    ),

    -- Logical CTEs
    completed_payments as (
        select 
            payments.orderid as order_id, 
            max(payments.created) as payment_finalized_date, 
            sum(payments.amount) / 100.0 as total_amount_paid
        from 
            payments
        where 
            status <> 'fail'
        group by 1
    ),

    paid_orders as (
        select 
            orders.id as order_id,
            orders.user_id    as customer_id,
            orders.order_date as order_placed_at,
            orders.status as order_status,
            completed_payments.total_amount_paid,
            completed_payments.payment_finalized_date,
            customers.first_name as customer_first_name,
            customers.last_name as customer_last_name
        from
            orders
        left join completed_payments on orders.id = completed_payments.order_id
        left join customers on orders.user_id = customers.id
    ),

    customer_value as (
        select
            p.order_id,
            sum(t2.total_amount_paid) as clv_bad
        from paid_orders p
        left join paid_orders t2 on p.customer_id = t2.customer_id and p.order_id >= t2.order_id
        group by 1
        order by p.order_id
    ),

    -- Final CTE
    final_ as (
        select
            paid_orders.*,
            row_number() over (order by paid_orders.order_id) as transaction_seq,
            row_number() over (partition by customer_id order by paid_orders.order_id) as customer_sales_seq,
            case when 
                rank() over (
                    partition by paid_orders.customer_id
                    order by paid_orders.order_placed_at, paid_orders.order_id
                ) = 1
            then 
                'new'
            else 
                'return' 
            end as nvsr,
            customer_value.clv_bad as customer_lifetime_value,
            first_value(paid_orders.order_placed_at) over (
                partition by paid_orders.customer_id 
                order by paid_orders.order_placed_at
            ) as fdos
        from paid_orders
        left join customer_value on customer_value.order_id = paid_orders.order_id
        order by order_id
    )

select 
    *
from 
    final_
