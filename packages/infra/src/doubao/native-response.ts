import type { ApprovalResponse } from "@aster/core";

/** Runs in Doubao's renderer. Resolve by observed module contract, never by guessed HTTP endpoints. */
export async function respondInRenderer(
  args: { request: any; response: ApprovalResponse },
  host: any = globalThis,
): Promise<void> {
  const chunks = host["@flow-web/desktop:stable"];
  if (!chunks) throw new Error("Unsupported Doubao desktop runtime");
  const req: any = await new Promise((resolve) =>
    chunks.push([[`signals_approval_${Date.now()}_${Math.random()}`], {}, resolve]),
  );
  const module = (matches: (source: string) => boolean) => {
    const ids = Object.entries(req.m).filter(([, value]) => matches(String(value)));
    if (ids.length !== 1) throw new Error("Doubao native response module unavailable or ambiguous");
    return req(ids[0]![0]);
  };
  const { request, response } = args;
  const communication = () => {
    const wrapper = Object.values(req.m)
      .map(String)
      .find(
        (source) =>
          source.includes("cua.local_file.sandbox_instance.update_conversation") &&
          source.length < 1500,
      );
    const id = wrapper?.match(/var \w+=\w+\((\d+)\)/)?.[1];
    if (!id) throw new Error("Doubao communication module unavailable");
    return req(id)._();
  };
  if (request.kind === "input") {
    if (!request.clarifyId || !Array.isArray(request.questions) || !request.questions.length)
      throw new Error("Missing native clarification identity");
    const questions = request.questions.map((question: any) => {
      const values =
        response.answers?.[question.question_id] ??
        (request.questions.length === 1 && response.text ? [response.text] : []);
      if (!values.length) throw new Error("Every question requires an explicit answer");
      if (question.type === 3)
        return {
          ...question,
          answer: {
            question_id: question.question_id,
            status: 2,
            selected_option_ids: [],
            capability_answer: { text: values.join("\n") },
          },
        };
      if (![1, 2].includes(question.type)) throw new Error("Unsupported native question type");
      const ids = values.map((value: string) => {
        const option = question.options?.find(
          (option: any) =>
            option.option_id === value || option.text === value || option.title === value,
        );
        if (!option) throw new Error("Answer does not match an offered option");
        return option.option_id;
      });
      if (question.type === 1 && ids.length !== 1)
        throw new Error("Single-choice question requires one answer");
      return {
        ...question,
        answer: {
          question_id: question.question_id,
          status: 2,
          selected_option_ids: [...new Set(ids)],
        },
      };
    });
    const bridge = module(
      (source) => source.includes("interaction_ask_submit_rpc_accepted") && source.includes("mH:"),
    );
    const handled = await bridge.mH({ clarify_id: request.clarifyId, status: 2, questions });
    if (!handled) throw new Error("Native clarification is not attached to a local task");
    return;
  }
  if (request.kind !== "approval" || !response.decision)
    throw new Error("An explicit approval decision is required");
  // These are the native single-command allow/reject actions; never grant session-wide permission.
  const type = response.decision === "approve" ? 1010 : 1011;
  const candidates = (request.items ?? []).filter((item: any) => Number(item.action_type) === type);
  if (candidates.length !== 1)
    throw new Error("Native approval does not offer an unambiguous allow/reject action");
  const item = candidates[0];
  const payload =
    typeof item.schema_payload === "string" ? JSON.parse(item.schema_payload) : item.schema_payload;
  const server = payload?.server_option;
  if (Number(server?.scene) === 11 && server?.callback_id) {
    const api = module(
      (source) => source.includes("GE:") && source.includes("Sf:") && source.includes("SI:"),
    );
    await api.GE.AGWUploadAskHumanWarnResult({
      callback_id: server.callback_id,
      scene: server.scene,
      params: JSON.stringify({ approve: response.decision === "approve" }),
    });
    return;
  }
  const toolCallId =
    payload?.tool_call_id ??
    payload?.toolCallId ??
    item.extra_params?.tool_call_id ??
    item.extra_params?.toolCallId;
  if (!toolCallId) throw new Error("Missing native tool-call identity");
  const comm = communication();
  const binding = await comm.invoke("cua.bash_escalation.quick_reply.resolve_binding", {
    toolCallId,
  });
  if (!binding?.handled || !binding.taskId)
    throw new Error("Native command approval binding is unavailable");
  const result = await comm.invoke("cua.bash_escalation.quick_reply.submit_click", {
    taskId: binding.taskId,
    toolCallId,
    actionId: response.decision === "approve" ? "allow" : "reject",
  });
  if (!result?.handled) throw new Error("Native command approval was not accepted");
}

/** No session navigation or foreground clicks. The caller verifies the pending control against its original run. */
export const respondDoubaoNative = async (
  request: any,
  response: ApprovalResponse,
  signal?: AbortSignal,
): Promise<void> => {
  const endpoint = (process.env.DOUBAO_CDP_ENDPOINT ?? "http://127.0.0.1:9226").replace(/\/$/, "");
  const targets = (await fetch(`${endpoint}/json/list`, {
    signal: signal
      ? AbortSignal.any([signal, AbortSignal.timeout(5000)])
      : AbortSignal.timeout(5000),
  }).then((response) => response.json())) as {
    url: string;
    webSocketDebuggerUrl?: string;
    type: string;
  }[];
  const target = targets.find((target) => {
    try {
      const url = new URL(target.url);
      return (
        target.type === "page" &&
        ["doubaowork:", "chrome:"].includes(url.protocol) &&
        url.hostname === "doubaowork-chat" &&
        /^\/chat(?:\/|$)/.test(url.pathname)
      );
    } catch {
      return false;
    }
  });
  if (!target?.webSocketDebuggerUrl) throw new Error("Doubao Work renderer is unavailable");
  await new Promise<void>((resolve, reject) => {
    let finished = false;
    const socket = new WebSocket(target.webSocketDebuggerUrl!);
    const timer = setTimeout(
      () => finish(new Error("Native approval response timed out; outcome may be unknown")),
      30_000,
    );
    const abort = () =>
      finish(new Error("Native approval response interrupted; outcome may be unknown"));
    const finish = (error?: Error) => {
      if (finished) return;
      finished = true;
      clearTimeout(timer);
      signal?.removeEventListener("abort", abort);
      socket.close();
      if (error) reject(error);
      else resolve();
    };
    signal?.addEventListener("abort", abort, { once: true });
    if (signal?.aborted) {
      abort();
      return;
    }
    socket.addEventListener("open", () =>
      socket.send(
        JSON.stringify({
          id: 1,
          method: "Runtime.evaluate",
          params: {
            expression: `(${respondInRenderer.toString()})(${JSON.stringify({ request, response })})`,
            awaitPromise: true,
            returnByValue: true,
          },
        }),
      ),
    );
    socket.addEventListener("close", () => {
      if (!finished) finish(new Error("Native approval connection closed before acknowledgement"));
    });
    socket.addEventListener("error", () => finish(new Error("Native approval connection failed")), {
      once: true,
    });
    socket.addEventListener("message", (event) => {
      const value = JSON.parse(String(event.data));
      if (value.id !== 1) return;
      if (value.error || value.result?.exceptionDetails)
        finish(
          new Error(
            value.result?.exceptionDetails?.exception?.description ??
              value.error?.message ??
              "Native approval failed",
          ),
        );
      else finish();
    });
  });
};
