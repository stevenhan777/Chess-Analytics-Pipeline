-- models/staging/stg_lichess__moves.sql
with moves as (

    select
        game_id,
        move_offset,
        move_number,
        color,
        lower(username) as username,
        move_san
    from {{ source('lichess_raw', 'moves') }}

),

evals as (

    select
        game_id,
        move_offset,
        eval_score,
        mate_in,
        best_move,
        variation,
        judgment,
        time_left_cs,
        time_left_ratio
    from {{ source('lichess_raw', 'move_evals') }}

)

-- Simple JOIN on the composite key
select
    m.game_id,
    m.move_offset,
    m.move_number,
    m.color,
    m.username,
    m.move_san,
    e.eval_score,
    e.mate_in,
    e.best_move,
    e.variation,
    e.judgment,
    e.time_left_cs,
    e.time_left_ratio

from moves m
left join evals e
    on  m.game_id = e.game_id
    and m.move_offset = e.move_offset