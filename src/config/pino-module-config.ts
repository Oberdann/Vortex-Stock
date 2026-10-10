import { Params } from "nestjs-pino";

export const loggerConfig: Params = {
  pinoHttp: {
    level: "info",
    base: { service: "stock-service" },
    serializers: { res: (res) => ({ statusCode: res.statusCode }) },
    customLogLevel: (_req, res, err) => {
      if (res.statusCode >= 500 || err) return "error";
      if (res.statusCode >= 400) return "warn";
      return "info";
    },
  },
};
