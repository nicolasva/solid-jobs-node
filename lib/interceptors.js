'use strict';

// Ordered middleware chain. An interceptor is `async (context, proceed) => result` or `{ around(context, proceed) }`.
// Not calling `proceed` short-circuits the chain (e.g. a publish interceptor returning null drops the task).
class InterceptorRegistry {
  constructor() {
    this.items = [];
  }

  use(interceptor) {
    if (!this.items.includes(interceptor)) this.items.push(interceptor);
    return this;
  }

  discard(interceptor) {
    this.items = this.items.filter((item) => item !== interceptor);
    return this;
  }

  empty() {
    return this.items.length === 0;
  }

  clear() {
    this.items = [];
  }

  async call(context, terminal) {
    const run = (index) => {
      if (index >= this.items.length) return terminal();
      const item = this.items[index];
      const around = typeof item === 'function' ? item : item.around.bind(item);
      return around(context, () => run(index + 1));
    };
    return run(0);
  }
}

module.exports = { InterceptorRegistry };
