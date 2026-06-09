-- models/marts/dim_openings.sql

with matches as (

    select * from {{ ref('fct_matches') }}

),

/*
  For each ECO + name combination, take the most-common opening_moves_string
  as the "canonical" move sequence (different games may have slightly
  different depths for the same ECO code).
*/
canonical_moves as (

    select
        opening_eco,
        opening_name,
        opening_moves_string,
        count(*) as games_with_this_sequence,
        row_number() over (
            partition by opening_eco, opening_name
            order by count(*) desc
        ) as rn
    from matches
    where opening_eco is not null
    group by opening_eco, opening_name, opening_moves_string

),

opening_stats as (

    select
        opening_eco,
        opening_name,

        -- totals
        count(*) as times_played,
        countif(result = 'white') as white_wins,
        countif(result = 'black') as black_wins,
        countif(result = 'draw') as draws,

        -- win rates
        safe_divide(countif(result = 'white'), count(*)) as white_win_rate,
        safe_divide(countif(result = 'black'), count(*)) as black_win_rate,
        safe_divide(countif(result = 'draw'),  count(*)) as draw_rate,

        -- game length characteristics
        round(avg(total_plies) / 2, 1) as avg_full_moves,
        round(avg(game_duration_sec) / 60, 1) as avg_duration_min,
        round(avg(max_eval_swing), 2) as avg_sharpness,
        round(avg(white_blunders + black_blunders), 2) as avg_blunders_per_game

    from matches
    where opening_eco is not null
    group by opening_eco, opening_name

)

select
    os.*,
    cm.opening_moves_string as canonical_moves
from opening_stats os
left join canonical_moves cm
    on os.opening_eco  = cm.opening_eco
   and os.opening_name = cm.opening_name
   and cm.rn = 1
order by os.times_played desc