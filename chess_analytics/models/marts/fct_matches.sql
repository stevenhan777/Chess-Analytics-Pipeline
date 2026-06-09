-- models/marts/fct_matches.sql

with games as (

    select * from {{ ref('stg_lichess__games') }}

),

moves as (

    select * from {{ ref('int_move_sequences') }}

),

openings as (

    select * from {{ ref('int_opening_patterns') }}

),

/*
  Aggregate move-level metrics up to the game level.
  Split by color so we can report white/black error counts separately.
*/
move_agg as (

    select
        game_id,

        -- total move counts
        count(*) as total_plies,
        countif(color = 'white') as white_plies,
        countif(color = 'black') as black_plies,

        -- error counts per color
        countif(color = 'white' and judgment = 'Blunder') as white_blunders,
        countif(color = 'black' and judgment = 'Blunder') as black_blunders,
        countif(color = 'white' and judgment = 'Mistake') as white_mistakes,
        countif(color = 'black' and judgment = 'Mistake') as black_mistakes,
        countif(color = 'white' and judgment = 'Inaccuracy') as white_inaccuracies,
        countif(color = 'black' and judgment = 'Inaccuracy') as black_inaccuracies,

        -- captures & checks
        countif(is_capture) as total_captures,
        countif(is_check) as total_checks,
        countif(is_castling) as total_castles,

        -- sharpness: average absolute eval (high = lopsided positions)
        avg(abs(eval_score)) as avg_eval_magnitude,
        max(abs(eval_score)) as max_eval_swing,

        -- time pressure
        avg(time_left_ratio) as avg_time_left_ratio,
        min(time_left_ratio) as min_time_left_ratio

    from moves
    group by game_id

)

select
    -- ── identifiers ──────────────────────────────────────────────────────
    g.game_id,
    g.white_username,
    g.black_username,

    -- ── ratings ──────────────────────────────────────────────────────────
    g.white_rating,
    g.black_rating,
    g.white_rating_diff,
    g.black_rating_diff,
    abs(g.white_rating - g.black_rating) as rating_gap,

    -- ── accuracy ─────────────────────────────────────────────────────────
    g.white_accuracy,
    g.black_accuracy,

    -- ── result ───────────────────────────────────────────────────────────
    g.result,
    g.status,

    -- ── opening ──────────────────────────────────────────────────────────
    g.opening_eco,
    g.opening_name,
    op.opening_moves_string,

    -- ── time control ─────────────────────────────────────────────────────
    g.perf_type,
    g.time_control_label,
    g.time_control_initial,
    g.time_control_increment,

    -- ── timing ───────────────────────────────────────────────────────────
    g.game_created_at,
    g.game_ended_at,
    timestamp_diff(g.game_ended_at, g.game_created_at, second) as game_duration_sec,
    date(g.game_created_at) as game_date,

    -- ── move metrics (from move_agg) ──────────────────────────────────────
    ma.total_plies,
    ma.white_plies,
    ma.black_plies,
    ma.white_blunders,
    ma.black_blunders,
    ma.white_mistakes,
    ma.black_mistakes,
    ma.white_inaccuracies,
    ma.black_inaccuracies,
    ma.total_captures,
    ma.total_checks,
    ma.total_castles,
    ma.avg_eval_magnitude,
    ma.max_eval_swing,
    ma.avg_time_left_ratio,
    ma.min_time_left_ratio

from games g
left join move_agg ma on g.game_id = ma.game_id
left join openings op on g.game_id = op.game_id