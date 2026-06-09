-- models/intermediate/int_opening_patterns.sql

with moves as (

    select
        m.game_id,
        m.move_offset,
        m.move_san,
        g.opening_ply,
        g.opening_eco,
        g.opening_name
    from {{ ref('stg_lichess__moves') }} m
    inner join {{ ref('stg_lichess__games') }} g
        on m.game_id = g.game_id

),

opening_moves as (

    -- Keep only moves that fall within Lichess's own opening classification depth
    select
        game_id,
        opening_eco,
        opening_name,
        move_san,
        move_offset
    from moves
    where move_offset < opening_ply

),

aggregated as (

    select
        game_id,
        opening_eco,
        opening_name,
        -- Reconstruct the opening move sequence as a single string, e.g.
        -- "e4 e5 Nf3 Nc6 Bb5 a6"  ← Ruy Lopez main line
        string_agg(move_san, ' ' order by move_offset) as opening_moves_string,
        count(*) as opening_move_count
    from opening_moves
    group by game_id, opening_eco, opening_name

)

select * from aggregated