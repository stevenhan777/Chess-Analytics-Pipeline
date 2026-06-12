-- models/staging/stg_lichess__games.sql
with source as (

    select * from {{ source('lichess_raw', 'games') }}

),

cleaned as (

    select
        game_id,
        lower(white_username) as white_username,
        lower(black_username) as black_username,
        white_rating,
        black_rating,
        white_rating_diff,
        black_rating_diff,
        white_accuracy,
        black_accuracy,
        winner,
        coalesce(winner, 'draw') as result,
        status,
        perf_type,
        coalesce(variant, 'standard') as variant,
        opening_eco,
        opening_name,
        opening_ply,
        time_control_initial,
        time_control_increment,
        concat(
            cast(time_control_initial / 60 as string), '+',
            cast(time_control_increment as string)
        ) as time_control_label,
        timestamp_millis(created_at) as game_created_at,
        timestamp_millis(last_move_at) as game_ended_at,
        ingested_at

    from source
    where time_control_initial > 0   -- exclude untimed games

)

select * from cleaned