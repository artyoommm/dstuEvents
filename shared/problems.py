from typing import Optional
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
from fastapi.exceptions import RequestValidationError
from starlette.exceptions import HTTPException as StarletteHTTPException
from pydantic import BaseModel


class ProblemDetail(BaseModel):
    type: str = "about:blank"
    title: str
    status: int
    detail: Optional[str] = None
    instance: Optional[str] = None
    correlationId: Optional[str] = None


class ProblemException(Exception):
    def __init__(self, status: int, title: str, detail: str | None = None,
                 type: str = "about:blank"):
        self.problem = ProblemDetail(type=type, title=title, status=status, detail=detail)


def _resp(request: Request, p: ProblemDetail) -> JSONResponse:
    p.instance = str(request.url.path)
    p.correlationId = request.headers.get("X-Correlation-Id")
    return JSONResponse(status_code=p.status, content=p.model_dump())


async def _problem_handler(request: Request, exc: ProblemException):
    return _resp(request, exc.problem)


async def _http_exc_handler(request: Request, exc: StarletteHTTPException):
    return _resp(request, ProblemDetail(
        status=exc.status_code,
        title=exc.detail if isinstance(exc.detail, str) else "HTTP error",
        type=f"https://dstu-events.local/errors/http-{exc.status_code}",
    ))


async def _validation_handler(request: Request, exc: RequestValidationError):
    return _resp(request, ProblemDetail(
        type="https://dstu-events.local/errors/validation",
        title="Validation error",
        status=422,
        detail=str(exc.errors()),
    ))


def install_problem_handlers(app: FastAPI):
    app.add_exception_handler(ProblemException, _problem_handler)
    app.add_exception_handler(StarletteHTTPException, _http_exc_handler)
    app.add_exception_handler(RequestValidationError, _validation_handler)