# Telegram Learner → Algorithm Integration Brief
Generated: `2026-05-04T20:37:29`

## Current learner signal
- `@darkhorseofstockmarket`: confidence 50.0, action `skip_or_observe`, sizing 0.1x, equity N=0 ret5=None max20=None, options N=0
- `@darkhorseofstockmarket_options`: confidence 39.2, action `skip_or_observe`, sizing 0.1x, equity N=0 ret5=None max20=None, options N=3
- `@financewithsunil`: confidence 42.9, action `skip_or_observe`, sizing 0.1x, equity N=9 ret5=-1.31 max20=6.19, options N=4
- `@milind4profits`: confidence 44.3, action `skip_or_observe`, sizing 0.1x, equity N=13 ret5=2.45 max20=6.25, options N=0
- `@shortterm01`: confidence 41.4, action `skip_or_observe`, sizing 0.1x, equity N=2 ret5=-3.14 max20=3.26, options N=0

## Audit-derived channel behavior
- `@FinanceWithSunil`: evaluated 9 signals; ret5 avg=-1.31 pos=0.0%; max20 avg=6.19 pos=80.0%; symbols=AETHER, BBOX, GESHIP, NTPCGREEN, SCHNEIDER, SUNFLAG, TANLA, UJJIVANSFB
- `@FinanceWithSunil`: evaluated 4 signals; ret5 avg=None pos=None%; max20 avg=None pos=None%; symbols=INDIGO, OFSS, SBILIFE
- `@Shortterm01`: evaluated 2 signals; ret5 avg=-3.14 pos=0.0%; max20 avg=3.26 pos=100.0%; symbols=DOMS, MARINE
- `@DarkHorseOfStockMarket`: evaluated 0 signals; ret5 avg=None pos=None%; max20 avg=None pos=None%; symbols=
- `@DarkHorseOfStockMarket`: evaluated 3 signals; ret5 avg=None pos=None%; max20 avg=None pos=None%; symbols=HAL, INDIGO
- `@Milind4Profits`: evaluated 13 signals; ret5 avg=2.45 pos=50.0%; max20 avg=6.25 pos=100.0%; symbols=ANURAS, BHARATWIRE, GENUSPOWER, GRANULES, GRSE, HUDCO, RADICO, RRKABEL, SKYGOLD, SPAL, STAR, TARIL, TIPSMUSIC

## Algorithm ideas to test
- `telegram_watchlist_boost`: if a symbol appears in a positive/high-MFE learner channel, reduce selected RS7 entry strictness only in paper/lab, not live full-size.
- `telegram_fast_profit_exit`: high max favorable but weak/unknown close returns means test earlier partial exits, trailing stops, and time-stop exits after Telegram-style momentum calls.
- `telegram_channel_weight`: use channel confidence as a confluence feature/position cap, not as an entry trigger. Low-confidence remains 0.1x observe.
- `telegram_gate_diff`: compare each Telegram call timestamp with RS7 diagnostics to see which gates blocked a later winner.

## Preliminary research universe
AETHER,ANURAS,BBOX,BHARATWIRE,GENUSPOWER,GESHIP,GRANULES,GRSE,HUDCO,NTPCGREEN,RADICO,RRKABEL,SCHNEIDER,SKYGOLD,SPAL,STAR,SUNFLAG,TANLA,TARIL,TIPSMUSIC,UJJIVANSFB
