# Strategy Filters And Metadata Resilience Design

## Goal

Improve the strategy run stock-pool filters so they behave like checkbox-based multi-select controls, and make stock metadata synchronization safer when the upstream board data source is unstable.

## UI Design

The strategy filter panel will render industry boards, concept boards, and markets as checkbox groups instead of native multi-select boxes. Each group will include compact actions for selecting all options and clearing all options. Markets default to all selected, and industry/concept groups also default to all selected after options load.

The filter payload remains unchanged: `industry_board_codes`, `concept_board_codes`, and `markets` continue to be arrays, so the backend contract and strategy execution flow stay stable.

The listed-days field will mention that `0` means no restriction, values above `60` help exclude very new listings, and values above `250` help exclude stocks listed within roughly one trading year. The 20-day average turnover field will keep the existing unit of `万元` and mention that `0` means no restriction, `5000` means roughly 50 million CNY, and `10000` means roughly 100 million CNY.

## Data Sync Design

Local stock codes are enough to infer exchange, but not industry or concept membership. Those memberships still need an external mapping source. The current AkShare/Eastmoney path will remain the primary source, but failures will be contained:

- Failure to fetch a board list returns an empty fetch result instead of crashing the job.
- Failure to fetch one board's constituents skips that board and records the failure count.
- Existing `stock_boards` and `stock_board_members` rows are preserved when a board type fetch returns no usable board/member data.
- The stock-basic board cache is refreshed from whatever local board membership data remains available.

## Testing

Add focused DataEngine tests for metadata sync resilience, then run the relevant backend tests. The frontend change will be verified by loading the local WebUI and checking that checkbox groups render and collect the expected payload shape.
