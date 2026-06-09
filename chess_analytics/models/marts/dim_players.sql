-- models/marts/dim_players.sql

with trajectory as (

    select * from {{ ref('int_rating_trajectories') }}
    where username is not null

),

/*
  Aggregate all games per player (across all time controls combined).
*/
stats as (

    select
        username,

        -- ── volume ─────────────────────────────────────────────────────────
        count(*) as total_games,
        countif(game_result = 'win') as wins,
        countif(game_result = 'draw') as draws,
        countif(game_result = 'loss') as losses,
        safe_divide(countif(game_result = 'win'), count(*)) as win_rate,

        -- ── color splits ──────────────────────────────────────────────────
        countif(color = 'white') as white_games,
        countif(color = 'black') as black_games,
        safe_divide(
            countif(color = 'white' and game_result = 'win'),
            nullif(countif(color = 'white'), 0)
        ) as white_win_rate,
        safe_divide(
            countif(color = 'black' and game_result = 'win'),
            nullif(countif(color = 'black'), 0)
        ) as black_win_rate,

        -- ── rating ─────────────────────────────────────────────────────────
        round(avg(rating), 0) as avg_rating,
        max(rating) as peak_rating,
        min(rating) as lowest_rating,
        -- Estimate current rating as last known value
        array_agg(
            rating order by game_created_at desc limit 1
        )[safe_offset(0)] as current_rating,

        -- ── accuracy ───────────────────────────────────────────────────────
        round(avg(accuracy), 1) as avg_accuracy,
        round(max(accuracy), 1) as best_accuracy,

        -- ── activity ───────────────────────────────────────────────────────
        min(game_created_at) as first_game_at,
        max(game_created_at) as last_game_at

    from trajectory
    group by username

)

select
    *,
    if(white_games >= black_games, 'white', 'black') as preferred_color,
    date_diff(
        date(last_game_at),
        date(first_game_at),
        day
    ) as active_days
from stats