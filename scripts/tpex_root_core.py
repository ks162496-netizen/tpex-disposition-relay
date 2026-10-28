"""Pro1530 core contracts. No credentials, database, network or publication binding.

Transport and atomic publishing are explicit ports. The bundled subprocess
transport is bounded; native source completeness/calendar/Git CAS remain unbound.
"""
from dataclasses import dataclass
from datetime import date, datetime, timezone
from email.utils import parsedate_to_datetime
import copy
import json
import math
import re
import random
import time


class PermanentError(RuntimeError):
    pass


class SchemaError(PermanentError):
    pass


class TransientError(RuntimeError):
    def __init__(self, reason, partial_bytes=None):
        super().__init__(reason)
        self.partial_bytes = partial_bytes


class BudgetExceeded(RuntimeError):
    pass


@dataclass
class Budget:
    deadline: float
    maximum_bytes: int
    clock: object = time.monotonic
    used_bytes: int = 0

    def __post_init__(self):
        if type(self.maximum_bytes) is not int or self.maximum_bytes < 1:
            raise ValueError('positive finite job body-byte budget required')
        if type(self.deadline) not in (int, float) or not math.isfinite(self.deadline):
            raise ValueError('finite job deadline required')

    def remaining(self):
        return max(0.0, self.deadline-self.clock())

    def charge(self, amount):
        if type(amount) is not int or amount < 0:
            raise PermanentError('unaccountable transport bytes')
        self.used_bytes += amount
        if self.used_bytes > self.maximum_bytes:
            raise BudgetExceeded('job bytes exhausted')


@dataclass(frozen=True)
class FetchPolicy:
    attempts: int = 3
    attempt_seconds: float = 20.0
    endpoint_seconds: float = 60.0
    attempt_bytes: int = 16*1024*1024

    def validate(self):
        if type(self.attempts) is not int or not 1 <= self.attempts <= 3:
            raise ValueError('at most three total attempts')
        if any(type(value) not in (int, float) or not math.isfinite(value) or value <= 0
               for value in [self.attempt_seconds, self.endpoint_seconds]):
            raise ValueError('positive finite time bounds required')
        if type(self.attempt_bytes) is not int or self.attempt_bytes < 1:
            raise ValueError('positive body byte limit required')


@dataclass(frozen=True)
class Response:
    status: int
    body: bytes
    headers: dict
    final_url: str


def retry_after_seconds(value, now):
    if not isinstance(value, str) or not value.strip():
        raise PermanentError('429 requires a valid Retry-After')
    value = value.strip()
    if re.fullmatch('[0-9]+', value):
        return int(value)
    try:
        stamp = parsedate_to_datetime(value)
        if stamp.tzinfo is None:
            raise ValueError('naive HTTP date')
        return max(0.0, (stamp-now).total_seconds())
    except (TypeError, ValueError, OverflowError) as error:
        raise PermanentError('invalid Retry-After') from error


def decode_json(raw):
    def constant(value):
        raise SchemaError('non-finite JSON value')
    if raw.lstrip().lower().startswith((b'<html', b'<!doctype')):
        raise SchemaError('HTML is not a dataset')
    try:
        return json.loads(raw.decode('utf-8-sig'), parse_constant=constant)
    except (UnicodeError, json.JSONDecodeError) as error:
        raise TransientError('malformed or truncated JSON', partial_bytes=0) from error


def fetch(source_url, read_once, validate, budget, policy=FetchPolicy(), *,
          decode=decode_json, sleep=time.sleep, jitter=random.random,
          wall_now=lambda: datetime.now(timezone.utc)):
    """One retry owner; read_once MUST enforce the supplied time/body limits.

    The subprocess adapter enforces time via kill+wait. Tests use fake responses.
    Failed response bytes count against the same shared job budget as successes.
    """
    policy.validate()
    endpoint_deadline = min(budget.deadline, budget.clock()+policy.endpoint_seconds)
    attempts = 0
    while attempts < policy.attempts:
        remaining = min(endpoint_deadline-budget.clock(), budget.remaining())
        room = budget.maximum_bytes-budget.used_bytes
        if remaining <= 0 or room <= 0:
            raise BudgetExceeded('endpoint/job budget exhausted')
        timeout = min(policy.attempt_seconds, remaining)
        cap = min(policy.attempt_bytes, room)
        attempts += 1
        started = budget.clock()
        delay = None
        charged = False
        try:
            response = read_once(source_url, timeout, cap)
            if not isinstance(response, Response) or not isinstance(response.body, bytes):
                raise PermanentError('invalid transport port return')
            budget.charge(len(response.body)); charged = True
            if len(response.body) > cap:
                raise BudgetExceeded('single attempt body limit exceeded')
            if budget.clock()-started >= timeout or budget.clock() >= endpoint_deadline:
                raise BudgetExceeded('single attempt/endpoint deadline exceeded')
            headers = {str(key).lower(): value for key, value in response.headers.items()}
            if response.status == 429:
                delay = retry_after_seconds(headers.get('retry-after'), wall_now())
                raise TransientError('rate limited', partial_bytes=0)
            if response.status in (408, 425, 500, 502, 503, 504):
                raise TransientError('transient HTTP status', partial_bytes=0)
            if response.status != 200 or response.final_url != source_url:
                raise PermanentError('authentication/URL/permanent HTTP failure')
            if str(headers.get('content-encoding', 'identity')).strip().lower() not in ('', 'identity'):
                raise SchemaError('unexpected encoded representation')
            length = headers.get('content-length')
            if length is not None:
                if not isinstance(length, str) or not re.fullmatch('[0-9]+', length.strip()):
                    raise SchemaError('invalid/ambiguous Content-Length')
                if len(response.body) != int(length):
                    raise TransientError('body byte length mismatch', partial_bytes=0)
            value = decode(response.body)
            validate(value)
            if budget.clock() >= endpoint_deadline:
                raise BudgetExceeded('validation exceeded endpoint/job deadline')
            return value
        except BudgetExceeded:
            if not charged:
                budget.charge(cap)
            raise
        except TransientError as error:
            if not charged:
                # Unknown partial consumption is charged at the admitted cap.
                budget.charge(cap if error.partial_bytes is None else error.partial_bytes)
            if budget.clock() >= endpoint_deadline:
                raise BudgetExceeded('endpoint/job deadline exhausted') from error
            if attempts == policy.attempts:
                raise
            variation = jitter()
            if type(variation) not in (int, float) or not math.isfinite(variation) or not 0 <= variation <= 1:
                raise ValueError('jitter must be between zero and one')
            backoff = min(2**(attempts-1), 4)+variation
            wait = max(backoff, delay if delay is not None else 0)
            if wait >= min(endpoint_deadline-budget.clock(), budget.remaining()):
                raise BudgetExceeded('Retry-After/backoff exceeds remaining budget')
            sleep(wait)
    raise AssertionError('unreachable retry state')


def iso_day(value):
    if not isinstance(value, str) or len(value) != 10:
        raise SchemaError('exact ISO source date required')
    try:
        return date.fromisoformat(value)
    except ValueError as error:
        raise SchemaError('invalid source date') from error


def auxiliary_quality(payload, category, completed_days, last_completed, *, calendar_receipt=None):
    """Source data day, never local fetch/attempt time, determines allowed use."""
    if category not in ('display', 'daily_statistics'):
        return {'state': 'QUALIFICATION_UNKNOWN', 'display_allowed': False,
                'daily_scoring_allowed': False, 'age_completed_sessions': None}
    result = {'state': 'UNKNOWN', 'display_allowed': False,
              'daily_scoring_allowed': False, 'age_completed_sessions': None}
    if not isinstance(payload, dict) or not calendar_receipt:
        return result
    source_day = payload.get('source_publication_date')
    try:
        iso_day(source_day); iso_day(last_completed)
        if completed_days != sorted(set(completed_days)):
            return result
        if source_day not in completed_days or last_completed not in completed_days:
            return result
        age = completed_days.index(last_completed)-completed_days.index(source_day)
        if age < 0:
            return result
    except (SchemaError, TypeError, ValueError):
        return result
    result['age_completed_sessions'] = age
    if category == 'display':
        result.update(state='STALE_ALLOWED' if age <= 5 else 'HISTORICAL_ONLY',
                      display_allowed=age <= 5)
    else:
        result.update(state='STALE_ALLOWED' if age <= 1 else 'HISTORICAL_ONLY',
                      display_allowed=True, daily_scoring_allowed=age <= 1)
    return result


class BudgetView:
    def __init__(self, parent, seconds, maximum_bytes):
        if type(seconds) not in (int, float) or not math.isfinite(seconds) or not 0 < seconds <= 60:
            raise ValueError('bounded auxiliary seconds required')
        if type(maximum_bytes) is not int or maximum_bytes <= 0:
            raise ValueError('bounded auxiliary body bytes required')
        self.parent = parent
        self.clock = parent.clock
        self.deadline = min(parent.deadline, parent.clock()+seconds)
        self.maximum_bytes = min(maximum_bytes, parent.maximum_bytes-parent.used_bytes)
        self.used_bytes = 0

    def remaining(self):
        return min(self.parent.remaining(), max(0.0, self.deadline-self.clock()))

    def charge(self, amount):
        self.parent.charge(amount)
        self.used_bytes += amount
        if self.used_bytes > self.maximum_bytes:
            raise BudgetExceeded('auxiliary body budget exhausted')


def bounded_auxiliary(loaders, fallback, budget, quality, *,
                      auxiliary_seconds=30, auxiliary_bytes=16*1024*1024):
    """Never starts another auxiliary after its shared finite budget is spent.

    A loader uses the bounded transport with this same budget. A hung native
    operation must be killed+waited by the transport adapter, not a leaked thread.
    """
    budget = BudgetView(budget, auxiliary_seconds, auxiliary_bytes)
    result = {}
    for name, loader in loaders.items():
        try:
            if budget.remaining() <= 0 or budget.used_bytes >= budget.maximum_bytes:
                raise BudgetExceeded('auxiliary shared wait exhausted')
            value = loader(budget)
            result[name] = {'payload': copy.deepcopy(value), 'stale': False,
                            'quality': quality(name, value), 'refresh_error': None}
        except (TransientError, PermanentError, BudgetExceeded) as error:
            prior = fallback.get(name)
            result[name] = {'payload': copy.deepcopy(prior), 'stale': True,
                            'quality': quality(name, prior),
                            'refresh_error': type(error).__name__}
    return result


