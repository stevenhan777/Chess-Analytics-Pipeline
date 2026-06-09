-- models/intermediate/int_move_sequences.sql

with moves as (

    select * from {{ ref('stg_lichess__moves') }}

),

classified as (

    select
        *,

        -- ── piece type ──────────────────────────────────────────────────
        -- SAN: uppercase first char = piece; lowercase = pawn; O-O = castling
        case
            when move_san like 'O-O%' then 'king'    -- castling
            when left(move_san, 1) = 'N' then 'knight'
            when left(move_san, 1) = 'B' then 'bishop'
            when left(move_san, 1) = 'R' then 'rook'
            when left(move_san, 1) = 'Q' then 'queen'
            when left(move_san, 1) = 'K' then 'king'
            else 'pawn'
        end as piece_type,

        -- ── boolean move flags ──────────────────────────────────────────
        move_san like '%x%' as is_capture,
        move_san like '%+%' as is_check,
        move_san like '%#%' as is_checkmate,
        move_san like '%=%' as is_promotion,
        move_san like 'O-O%' as is_castling,

        -- ── eval delta: did this move change the position? ──────────────
        -- A positive delta (from the side-to-move perspective) = improvement.
        -- We use LAG to get the opponent's last eval, then flip the sign.
        eval_score - (
            -1 * lag(eval_score) over (
                partition by game_id
                order by move_offset
            )
        ) as eval_delta,

        -- ── eval volatility (std-dev of last 3 evals) ───────────────────
        -- Rough measure of position complexity / sharpness.
        stddev(eval_score) over (
            partition by game_id
            order by move_offset
            rows between 2 preceding and current row
        ) as eval_volatility_3

    from moves

)

select * from classified