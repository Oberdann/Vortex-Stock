export function Ok<T>(message = "Sucesso", data: T | [] = []) {
  return { message, data, success: true };
}
