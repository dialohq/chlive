// The WorkOS API, as much as the chat demo's setup and smoke test use: the key from
// WORKOS_API_KEY (never printed), and the roles that are the chat's permissions: a member's
// role in the organization (`chatter` may read the messages, `member` not), and `feed-reader`
// on the feeds whose messages they may read.

const base = process.env.WORKOS_URL ?? "https://api.workos.com";
const key = process.env.WORKOS_API_KEY;
if (!key) throw new Error("set WORKOS_API_KEY (a WorkOS API key of a test environment)");

/** The organization the chat's users are members of, by its external_id (`organization` in
 * nix/chlive.chat.toml). */
export const ORGANIZATION = "chlive-chat";

export type Json = Record<string, any>;

/** A request; a 404 is `undefined`, any other failure throws. */
export async function api(method: string, path: string, body?: unknown): Promise<Json | undefined> {
  const res = await fetch(base + path, {
    method,
    headers: { Authorization: `Bearer ${key}`, ...(body ? { "Content-Type": "application/json" } : {}) },
    body: body ? JSON.stringify(body) : undefined,
  });
  if (res.status === 404) return undefined;
  const text = await res.text();
  if (!res.ok) throw new Error(`${method} ${path}: ${res.status} ${text.slice(0, 300)}`);
  return text ? JSON.parse(text) : {};
}

const q = (params: Record<string, string>) => new URLSearchParams(params).toString();

export async function organization(): Promise<Json> {
  const org = await api("GET", `/organizations/external_id/${ORGANIZATION}`);
  if (!org) throw new Error(`no WorkOS organization with external_id ${ORGANIZATION} (run workos-setup)`);
  return org;
}

/** The chat user's (by external_id) membership in the organization. */
export async function membership(org: Json, user: string): Promise<string> {
  const u = await api("GET", `/user_management/users/external_id/${encodeURIComponent(user)}`);
  if (!u) throw new Error(`no WorkOS user ${user} (run workos-setup)`);
  const list = await api("GET", `/user_management/organization_memberships?${q({ user_id: u.id, organization_id: org.id })}`);
  const om = list?.data.find((m: Json) => m.status === "active");
  if (!om) throw new Error(`WorkOS user ${user} isn't a member of ${ORGANIZATION} (run workos-setup)`);
  return om.id;
}

/** The member's role in the organization (one at a time). */
export async function role(om: string): Promise<string> {
  const m = await api("GET", `/user_management/organization_memberships/${om}`);
  return m!.role.slug;
}

/** Give the member `slug` as their role in the organization; whether it changed. */
export async function setRole(om: string, slug: string) {
  if ((await role(om)) === slug) return false;
  await api("PUT", `/user_management/organization_memberships/${om}`, { role_slug: slug });
  return true;
}

const feed = (id: string) => ({ role_slug: "feed-reader", resource_type_slug: "feed", resource_external_id: id });

export async function readsFeed(om: string, id: string): Promise<boolean> {
  const list = await api("GET", `/authorization/organization_memberships/${om}/role_assignments?limit=100`);
  return (list?.data ?? []).some(
    (a: Json) => a.role.slug === "feed-reader" && a.resource.resource_type_slug === "feed" && a.resource.external_id === id,
  );
}

/** Let the member read `id`'s feed; whether it changed. */
export async function grantFeed(om: string, id: string) {
  if (await readsFeed(om, id)) return false;
  await api("POST", `/authorization/organization_memberships/${om}/role_assignments`, feed(id));
  return true;
}

/** Stop the member reading `id`'s feed; whether it changed. */
export async function revokeFeed(om: string, id: string) {
  if (!(await readsFeed(om, id))) return false;
  await api("DELETE", `/authorization/organization_memberships/${om}/role_assignments`, feed(id));
  return true;
}
