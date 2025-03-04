from control import *

config_dict = {
    'project_name': project_name, 
    'experiment_name':experiment_name,
    'time_delta': {
        'mode': time_delta_mode,
        'increment': time_delta_increment,
        'multiplicative': time_delta_multiplicative,
        'balanced': time_delta_balanced
    },
    'stop_loss': stop_loss,
    'take_profit': take_profit,
    'mode': mode,
    'train_period': {
        'start': train_period_start,
        'end': train_period_end
    },
    'test_period': {
        'start': test_period_start,
        'end': test_period_end
    },
    'train_tickers': train_tickers,
    'train_time_delta': {
        'start': train_time_delta,
        'mode': train_time_delta_mode,
        'increment': train_time_delta_increment,
        'multiplicative': train_time_delta_multiplicative,
        'balanced': train_time_delta_balanced
    },
    'train_suggestion_heap_limit': train_suggestion_heap_limit,
    'train_start_cash': train_start_cash,
    'train_trade_liquidity_limit': train_trade_liquidity_limit,
    'train_trade_asset_limit': train_trade_asset_limit,
    'train_rank_liquidity_limit': train_rank_liquidity_limit,
    'train_rank_asset_limit': train_rank_asset_limit,
    'train_profit': {
        'price_change_ratio_d1': train_profit_price_change_ratio_d1,
        'profit_time_d1': train_profit_profit_time_d1,
        'price_change_ratio_d2': train_profit_price_change_ratio_d2,
        'profit_time_d2': train_profit_profit_time_d2,
        'profit_time_else': train_profit_profit_time_else
    },
    'train_loss': {
        'price_change_ratio_d1': train_loss_price_change_ratio_d1,
        'profit_time_d1': train_loss_profit_time_d1,
        'price_change_ratio_d2': train_loss_price_change_ratio_d2,
        'profit_time_d2': train_loss_profit_time_d2,
        'profit_time_else': train_loss_profit_time_else
    },
    'train_stop_loss': train_stop_loss,
    'train_take_profit': train_take_profit,
    'rank_liquidity_limit': rank_liquidity_limit,
    'rank_asset_limit': rank_asset_limit,
    'profit': {
        'price_change_ratio_d1': profit_price_change_ratio_d1,
        'profit_time_d1': profit_profit_time_d1,
        'price_change_ratio_d2': profit_price_change_ratio_d2,
        'profit_time_d2': profit_profit_time_d2,
        'profit_time_else': profit_profit_time_else
    },
    'loss': {
        'price_change_ratio_d1': loss_price_change_ratio_d1,
        'profit_time_d1': loss_profit_time_d1,
        'price_change_ratio_d2': loss_price_change_ratio_d2,
        'profit_time_d2': loss_profit_time_d2,
        'profit_time_else': loss_profit_time_else
    },
    'trade_liquidity_limit': trade_liquidity_limit,
    'trade_asset_limit': trade_asset_limit,
    'suggestion_heap_limit': suggestion_heap_limit
}