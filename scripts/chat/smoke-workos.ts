// The chat stack end to end, with its permissions in WorkOS (`chat-smoke` in the chat shell,
// with `dnvr up`; after `workos-setup`): what each user may read comes from WorkOS, polled: the
// messages table (their role in the organization: `chatter` may, `member` not; a lost table ends
// the subscription), and whose messages (the feeds they're a `feed-reader` of); anonymous
// connections read nothing. Changes it makes in WorkOS are undone at the end.
import { as, closeAll, follow, has, message, MESSAGES } from "./live.ts";
import { grantFeed, membership, organization, revokeFeed, setRole } from "./workos-api.ts";

const org = await organization();
const [alice, dave] = [await membership(org, "alice"), await membership(org, "dave")];

// as set up: Alice doesn't read Carol's feed; Dave may not read messages
await revokeFeed(alice, "carol");
await setRole(dave, "member");

const aliceChat = follow(as("alice"), MESSAGES);
const bobChat = follow(as("bob"), MESSAGES);
await aliceChat.until(() => true, "alice's messages");
await bobChat.until(() => true, "bob's messages");

// a message from Carol: Bob reads her feed, Alice doesn't
const body = await message("carol");
await bobChat.until(has(body), "Carol's message in Bob's chat");
await Bun.sleep(500);
if (aliceChat.state!.rows.some((r) => r.user_id === "carol")) throw new Error("Alice sees Carol's messages");
console.log("ok: Bob sees Carol's message, Alice doesn't");

// Alice made a reader of Carol's feed in WorkOS: it comes into her chat (at the next poll)
await grantFeed(alice, "carol");
await aliceChat.until(has(body), "Carol's message in Alice's chat after the grant");
console.log("ok: granted, Alice sees it");

// and taken back: it goes
await revokeFeed(alice, "carol");
await aliceChat.until((s) => !s.rows.some((r) => r.user_id === "carol"), "Carol's messages gone from Alice's chat");
console.log("ok: revoked, it's gone");

// Dave, a `member`, may not read messages; a `chatter`, he may; a `member` again, his
// subscription ends
await follow(as("dave"), MESSAGES).fails("table messages isn't readable", "Dave's messages, denied");
await setRole(dave, "chatter");
// (a failed subscription is over: subscribe again, until chlive has asked WorkOS again: its
// answers are up to a `poll` old)
let daveChat;
for (let tries = 0; ; tries++) {
  daveChat = follow(as("dave"), MESSAGES);
  try {
    await daveChat.until((s) => s.rows.some((r) => r.user_id === "dave"), "Dave's messages as a chatter");
    break;
  } catch (e) {
    if (tries === 10) throw e;
    await Bun.sleep(1000);
  }
}
await setRole(dave, "member");
await daveChat.fails("table messages isn't readable", "Dave's live messages after the revoke");
console.log("ok: the messages table granted, then revoked live (the subscription ends)");

// anonymous: nothing
await follow(as(), MESSAGES).fails("needs a user", "anonymous messages");
console.log("ok: anonymous connections read nothing");

closeAll();
console.log("chat smoke (WorkOS): all ok");
