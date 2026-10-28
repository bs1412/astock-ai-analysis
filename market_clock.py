"""Shanghai wall clock, verified 2026 A-share sessions and price-time evidence.

No network/credentials. Schedule is not proof of an individual security trading
or a provider having current/complete prices. Unknown calendars remain unknown.
"""
from datetime import date,datetime,time,timedelta
from zoneinfo import ZoneInfo

SHANGHAI=ZoneInfo('Asia/Shanghai')
POLICY_VERSION='ashare_clock_2026_v1'
CALENDAR_YEARS=(2026,)
QUOTE_MAX_AGE_SECONDS=120
SOURCES={
 'sse_calendar':'https://www.sse.com.cn/disclosure/announcement/general/c/c_20251222_10802507.shtml',
 'szse_calendar':'https://www.szse.cn/disclosure/notice/general/t20251222_618087.html',
 'bse_calendar':'https://dataclouds.cninfo.com.cn/sjother2/regulatory/2025/20251222/55875a9937374da4ae2d4999dfda2722.pdf',
 'sse_session':'https://one.sse.com.cn/onething/gptz/',
 'szse_session':'https://www.szse.cn/www/investor/knowledge/stock/deal/t20191204_572383.html',
 'bse_session':'https://www.bse.cn/jygl_list/200028217.html'}
HOLIDAYS=(('2026-01-01','2026-01-03','元旦'),('2026-02-15','2026-02-23','春节'),
 ('2026-04-04','2026-04-06','清明节'),('2026-05-01','2026-05-05','劳动节'),
 ('2026-06-19','2026-06-21','端午节'),('2026-09-25','2026-09-27','中秋节'),
 ('2026-10-01','2026-10-07','国庆节'))
PHASES=((time(9,15),'pre_open','开盘竞价前'),(time(9,25),'opening_auction','开盘集合竞价'),
 (time(9,30),'auction_break','开盘竞价后等待连续交易'),(time(11,30),'continuous_am','上午连续交易'),
 (time(13),'lunch_break','午间休市'),(time(14,57),'continuous_pm','下午连续交易'),
 (time(15),'closing_auction','收盘集合竞价'),(time(15,5),'after_close','常规竞价已结束'),
 (time(15,30),'post_hours_window','盘后窗口（适用证券需核对）'),(time.max,'after_close','常规竞价已结束'))
ACTIVE={'opening_auction','continuous_am','continuous_pm','closing_auction'}


def now():return datetime.now(SHANGHAI)


def aware(value=None):
    value=now() if value is None else value
    if not isinstance(value,datetime) or value.tzinfo is None or value.utcoffset() is None:
        raise ValueError('market time requires an aware datetime')
    return value.astimezone(SHANGHAI)


def parse_time(value):
    if not value:return None
    try:return aware(datetime.fromisoformat(str(value)))
    except (TypeError,ValueError,OverflowError):return None


def parse_day(value):
    try:return datetime.fromisoformat(str(value)).date()
    except (TypeError,ValueError):return None


def day_status(value):
    day=value.date() if isinstance(value,datetime) else value if isinstance(value,date) else date.fromisoformat(str(value))
    holiday=next((name for first,last,name in HOLIDAYS if first<=day.isoformat()<=last),None)
    if day.weekday()>=5:return {'is_trading_day':False,'reason':'weekend','label':'周末休市','holiday':holiday}
    if day.year not in CALENDAR_YEARS:return {'is_trading_day':None,'reason':'calendar_unknown','label':'交易日历未核实','holiday':None}
    return {'is_trading_day':not bool(holiday),'reason':'holiday' if holiday else 'scheduled_session',
            'label':holiday+'休市' if holiday else '已登记交易日','holiday':holiday}


def snapshot(at=None):
    at=aware(at);day=at.date();status=day_status(day)
    boundary=datetime.combine(day+timedelta(days=1),time.min,SHANGHAI)
    if status['is_trading_day'] is False:phase,label=status['reason'],status['label']
    elif status['is_trading_day'] is None:phase,label='calendar_unknown',status['label']
    else:
        phase,label='after_close','常规竞价已结束'
        for end,code,name in PHASES:
            if at.time()<end:
                phase,label=code,name
                if end!=time.max:boundary=datetime.combine(day,end,SHANGHAI)
                break
    next_open=None
    for offset in range(16):
        following=day+timedelta(days=offset);check=day_status(following)
        if check['is_trading_day'] is None:break
        opening=datetime.combine(following,time(9,15),SHANGHAI)
        if check['is_trading_day'] and opening>at:next_open=opening.isoformat();break
    return {'server_time':at.isoformat(timespec='seconds'),'timezone':'Asia/Shanghai','date':day.isoformat(),
        'weekday':('星期一','星期二','星期三','星期四','星期五','星期六','星期日')[day.weekday()],
        **status,'phase':phase,'phase_label':label,'is_regular_matching_session':phase in ACTIVE,
        'next_phase_at':boundary.isoformat(),'next_open_at':next_open,'policy_version':POLICY_VERSION,
        'calendar_verified_years':list(CALENDAR_YEARS),'calendar_sources':SOURCES,
        'clock_basis':'server_system_clock_not_independently_network_synchronized',
        'note':'只描述常规股票竞价日程；不证明单股未停牌或报价及时。部分证券有盘后交易，须核对适用机制。'}


def daily_evidence(value,retrieved_at=None,at=None):
    at=aware(at);day=parse_day(value);state=day_status(day) if day else None
    if day is None:kind,label='unknown','日线日期未知'
    elif day>at.date():kind,label='future','未来日期日线，无法作为当前依据'
    elif state['is_trading_day'] is False:kind,label='nontrading_day','非交易日数据，无法作为完整日线'
    elif day==at.date():kind,label='unfinished_or_unconfirmed','当日日线价格（完整收盘未核实）'
    elif state['is_trading_day'] is None:kind,label='calendar_unknown','历史日线参考（交易日历未核实）'
    else:kind,label='completed_day_reference',day.isoformat()+'已结束交易日收盘参考'
    return {'kind':kind,'label':label,'source_trade_date':day.isoformat() if day else None,
        'retrieved_at':retrieved_at,'provider_completeness_confirmed':False,
        'is_elapsed_trading_session':kind=='completed_day_reference'}


def quote_evidence(source_time,retrieved_at=None,at=None):
    at=aware(at);clock=snapshot(at);source=parse_time(source_time);warnings=[]
    kind,label='unknown','报价时间未知'
    source_phase=None;age=None;usable=False
    if source is not None:
        age=(at-source).total_seconds();source_state=snapshot(source);source_phase=source_state['phase']
        if age<0:kind,label='future','报价时刻晚于服务器时间';warnings.append('未来时刻报价不可使用')
        elif source_state['is_trading_day'] is False:kind,label='nontrading_day','非交易日供应商报价';warnings.append('源日期休市，字段可能为缓存，不能作为当前价格')
        elif source_state['is_trading_day'] is None:kind,label='calendar_unknown','报价日历未核实'
        elif source.date()<at.date():kind,label='historical_reference','历史报价参考'
        else:
            kind=source_phase
            label={'opening_auction':'开盘集合竞价期间报价参考','auction_break':'开盘竞价后报价参考',
                'continuous_am':'盘中报价参考','continuous_pm':'盘中报价参考','lunch_break':'午休时段报价参考',
                'closing_auction':'收盘集合竞价期间报价参考','after_close':'常规收盘后供应商报价参考',
                'post_hours_window':'盘后窗口供应商报价参考','pre_open':'开盘前供应商报价参考'}.get(kind,'供应商报价参考')
        if 0<=age and source_state['is_trading_day'] is True:
            if clock['is_regular_matching_session']:
                usable=source.date()==at.date() and age<=QUOTE_MAX_AGE_SECONDS
            elif clock['is_trading_day'] is True and clock['phase']!='pre_open':
                usable=source.date()==at.date()
            else:usable=(at.date()-source.date()).days<=7
        if clock['is_regular_matching_session'] and (source.date()!=at.date() or age>QUOTE_MAX_AGE_SECONDS):
            warnings.append('未取得当前时段足够新的报价，参考估值未知')
        elif age>QUOTE_MAX_AGE_SECONDS:
            warnings.append('仅按所列源时刻作报价参考，不代表此刻可成交价格')
        if source_phase in ('opening_auction','closing_auction','auction_break'):
            warnings.append('供应商最新价字段不保证是虚拟匹配价、最终开收盘价或可成交价')
    else:warnings.append('报价缺少可核对的带时区源时刻，参考估值未知')
    return {'kind':kind,'label':label,'source_time':source.isoformat(timespec='seconds') if source else None,
        'retrieved_at':retrieved_at,'source_phase':source_phase,'age_seconds':round(age,1) if age is not None else None,
        'usable_for_valuation':usable,'max_active_age_seconds':QUOTE_MAX_AGE_SECONDS,
        'warnings':warnings,'is_verified_closing_price':False,'is_execution_evidence':False,'market_time':clock}


def prompt_context(at=None):
    state=snapshot(at)
    return ('服务器上海时间 '+state['server_time']+'，'+state['weekday']+'，市场阶段：'+state['phase_label']+'。'
        '必须分别说明当前时间、报价源时刻与已结束日线日期。集合竞价价/盘中价/当日未确认日线不能称今日收盘价；'
        '周末和公告休市不能说正在常规股票交易。旧报价不能称实时；未知日历或源时间写未知。'
        '日线close只是字段名，不证明今日已经收盘；历史收盘须带日期。系统时钟未独立联网校时，单股停牌/盘口仍须核对。')
