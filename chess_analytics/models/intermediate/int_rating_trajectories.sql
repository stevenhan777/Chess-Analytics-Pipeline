-- models/intermediate/int_rating_trajectories.sql

with games as (

    select
        game_id,
        game_created_at,
        white_username,
        black_username,
        white_rating,
        black_rating,
        white_rating_diff,
        black_rating_diff,
        white_accuracy,
        black_accuracy,
        result,
        perf_type
    from {{ ref('stg_lichess__games') }}

),

/*
  Pivot: one row per player per game, normalized to a common schema.
  This avoids repeating all window logic for white and black separately.
*/
player_games as (

    select
        game_id,
        game_created_at,
        white_username as username,
        'white' as color,
        white_rating as rating,
        white_rating_diff as rating_diff,
        white_accuracy as accuracy,
        case
            when result = 'white' then 'win'
            when result = 'black' then 'loss'
            else 'draw'
        end as game_result,
        perf_type
    from games

    union all

    select
        game_id,
        game_created_at,
        black_username as username,
        'black' as color,
        black_rating as rating,
        black_rating_diff as rating_diff,
        black_accuracy as accuracy,
        case
            when result = 'black' then 'win'
            when result = 'white' then 'loss'
            else 'draw'
        end as game_result,
        perf_type
    from games

),

with_trajectory as (

    select
        *,

        -- ── game sequence per player ─────────────────────────────────────
        row_number() over (
            partition by username, perf_type
            order by game_created_at
        ) as game_sequence,

        -- ── rating at previous game ──────────────────────────────────────
        lag(rating) over (
            partition by username, perf_type
            order by game_created_at
        ) as prev_rating,

        -- ── rating at next game (what this game earned) ──────────────────
        lead(rating) over (
            partition by username, perf_type
            order by game_created_at
        ) as next_rating,

        -- ── rolling 10-game win rate ──────────────────────────────────────
        avg(if(game_result = 'win', 1.0, 0.0)) over (
            partition by username, perf_type
            order by game_created_at
            rows between 9 preceding and current row
        ) as rolling_10_win_rate,

        -- ── rolling 10-game avg accuracy ─────────────────────────────────
        avg(accuracy) over (
            partition by username, perf_type
            order by game_created_at
            rows between 9 preceding and current row
        ) as rolling_10_avg_accuracy

    from player_games

)

select * from with_trajectory