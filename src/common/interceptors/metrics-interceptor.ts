import {
  CallHandler,
  ExecutionContext,
  Injectable,
  NestInterceptor,
} from "@nestjs/common";
import { Observable, tap } from "rxjs";
import {
  httpRequestDuration,
  httpRequestsTotal,
} from "../../config/prometheus-config";

@Injectable()
export class MetricsInterceptor implements NestInterceptor {
  intercept(context: ExecutionContext, next: CallHandler): Observable<any> {
    const startedAt = Date.now();
    const request = context.switchToHttp().getRequest();
    const response = context.switchToHttp().getResponse();

    return next.handle().pipe(
      tap(() => {
        const labels = {
          method: request.method,
          route: request.route?.path || request.url,
          status: response.statusCode,
        };
        httpRequestsTotal.inc(labels);
        httpRequestDuration.observe(labels, (Date.now() - startedAt) / 1000);
      }),
    );
  }
}
