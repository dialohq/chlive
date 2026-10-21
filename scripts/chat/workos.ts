// The chat demo's permissions in WorkOS (`workos-setup` in the chat shell): adds what's missing,
// changes nothing that's there, so it's safe to run again. Needs WORKOS_API_KEY, and the `feed`
// resource type (the dashboard's Authorization > Resource types: WorkOS has no API for those).
//
// - the organization `chlive-chat` (external_id `chlive-chat`)
// - `messages:read` on the organization, in the role `chatter`: who may read the messages table
//   (a member's role in the organization: `chatter`, or the default `member`)
// - `feed:read` on a feed, in the role `feed-reader`: whose messages (a feed per user, its
//   external_id the user's id, the `user_id` of their messages)
// - the users Alice, Bob, Carol and Dave (external_id: their chat id), members; everyone but
//   Dave a `chatter`; feed readers as the Postgres seed has it (added, never taken away)
import { api, grantFeed, membership, ORGANIZATION, role, setRole, type Json } from "./workos-api.ts";

const USERS = ["alice", "bob", "carol", "dave"];
const NAMES: Record<string, string> = { alice: "Alice", bob: "Bob", carol: "Carol", dave: "Dave" };
const CHATTERS = ["alice", "bob", "carol"];
const READS: Record<string, string[]> = {
  alice: ["alice", "bob"],
  bob: ["bob", "alice", "carol"],
  carol: USERS,
  dave: ["dave"],
};

const made: string[] = [];

async function ensure(what: string, get: () => Promise<Json | undefined>, create: () => Promise<unknown>) {
  if (await get()) return;
  await create();
  made.push(what);
}

// the organization: found by external_id, or by name (then given its external_id), or made
let org = await api("GET", `/organizations/external_id/${ORGANIZATION}`);
if (!org) {
  const list = await api("GET", `/organizations?${new URLSearchParams({ limit: "100" })}`);
  const named = list?.data.find((o: Json) => o.name === ORGANIZATION);
  org = named
    ? await api("PUT", `/organizations/${named.id}`, { name: named.name, external_id: ORGANIZATION })
    : await api("POST", "/organizations", { name: ORGANIZATION, external_id: ORGANIZATION });
  made.push(`organization ${ORGANIZATION}`);
}
org = org!;

// permissions and roles
for (const [permission, name, type] of [
  ["messages:read", "Read messages", undefined],
  ["feed:read", "Read feed", "feed"],
] as const) {
  try {
    await ensure(
      `permission ${permission}`,
      () => api("GET", `/authorization/permissions/${permission}`),
      () => api("POST", "/authorization/permissions", { slug: permission, name, ...(type ? { resource_type_slug: type } : {}) }),
    );
  } catch (e) {
    if (type === "feed") throw new Error(`${e}\n(is there a \`feed\` resource type? Make it in the WorkOS dashboard)`);
    throw e;
  }
}
for (const [role, name, permission, type] of [
  ["chatter", "Chatter", "messages:read", undefined],
  ["feed-reader", "Feed reader", "feed:read", "feed"],
] as const) {
  await ensure(
    `role ${role}`,
    () => api("GET", `/authorization/roles/${role}`),
    () => api("POST", "/authorization/roles", { slug: role, name, ...(type ? { resource_type_slug: type } : {}) }),
  );
  const r = await api("GET", `/authorization/roles/${role}`);
  if (!r!.permissions.includes(permission)) {
    await api("POST", `/authorization/roles/${role}/permissions`, { slug: permission });
    made.push(`${permission} in ${role}`);
  }
}

// users, their memberships and feeds
for (const user of USERS) {
  await ensure(
    `user ${user}`,
    () => api("GET", `/user_management/users/external_id/${user}`),
    () => api("POST", "/user_management/users", { email: `${user}@example.com`, external_id: user, first_name: NAMES[user] }),
  );
  const u = await api("GET", `/user_management/users/external_id/${user}`);
  await ensure(
    `${user}'s membership`,
    async () => {
      const list = await api("GET", `/user_management/organization_memberships?${new URLSearchParams({ user_id: u!.id, organization_id: org.id })}`);
      return list?.data[0];
    },
    () => api("POST", "/user_management/organization_memberships", { user_id: u!.id, organization_id: org.id }),
  );
  await ensure(
    `${user}'s feed`,
    () => api("GET", `/authorization/organizations/${org.id}/resources/feed/${user}`),
    () => api("POST", "/authorization/resources", { resource_type_slug: "feed", external_id: user, organization_id: org.id, name: `${NAMES[user]}'s feed` }),
  );
}

// who may read the messages, and whose
for (const user of USERS) {
  const om = await membership(org, user);
  if (CHATTERS.includes(user) && (await role(om)) === "member" && (await setRole(om, "chatter"))) made.push(`${user}: chatter`);
  for (const feed of READS[user]) {
    if (await grantFeed(om, feed)) made.push(`${user}: reads ${feed}'s feed`);
  }
}

console.log(made.length ? `made: ${made.join(", ")}` : "everything was there already");
