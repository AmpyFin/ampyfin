from pyrate_limiter import Duration, Limiter, RequestRate
from requests_ratelimiter import LimiterSession

limiter = LimiterSession(limiter=Limiter(RequestRate(2, Duration.SECOND)))
