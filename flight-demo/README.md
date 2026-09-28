# Atlas Air local demo

Open http://localhost:4173. The UI calls the real Atlas reservation APIs through a local server. Atlas Air, itineraries, fares and baggage copy are illustrative; there are no payments, airline integrations, numbered-seat assignments or tickets.

## Start and stop

Start Docker Desktop's Linux engine. In Windows PowerShell:

```powershell
# Run from the cloned repository root.
.\flight-demo\start.ps1
```

Wait until the page says Atlas connected. The script works in Windows PowerShell 5.1 and PowerShell 7. It creates private random credentials under `.tools/flight-demo/local.env`; the browser never receives those passwords. Build the application JAR first with the root Maven wrapper. Compose builds the app from the root Dockerfile and runs the UI with an official, digest-pinned Node.js image; no recording or rendering tools are required.

```powershell
.\flight-demo\stop.ps1
```

Stopping preserves the database volume. A confirmed reservation stays allocated. For a fresh demonstration without deleting data, stop the current project and start a new named project:

```powershell
.\flight-demo\stop.ps1
.\flight-demo\start.ps1 -ProjectName atlas-flight-fresh
# Later stop the same project:
.\flight-demo\stop.ps1 -ProjectName atlas-flight-fresh
```

Only one project can use port 4173 at a time. Use a new project name for a new dataset; reopening the same name preserves its bookings.

## Try it

1. As Traveller A, select AT101. Its only available capacity is held for 30 seconds.
2. Open a private browser window as Traveller B. The flight becomes unavailable. If B races a stale availability screen, Atlas rejects the hold with a capacity conflict.
3. Leave A's hold unfinished. After expiration commits, the place returns to availability.
4. As B, select AT101 and confirm within the deadline. The reservation is confirmed and capacity remains allocated.
5. Use AT204 or AT308 to try multi-passenger holds and cancellation. Only temporary holds can be cancelled here; confirmed capacity is retained by Atlas's contract.

The countdown follows server expiration; the page waits for the committed EXPIRED state rather than releasing capacity itself. Demo accounts A/B map to fixed local Atlas principals. Switching an account is a demo control, not production authentication. Browser-local history is not a complete booking list across devices.

This isolated demo enables PostgreSQL expiration and uses a 30-second hold duration. It leaves Redis and Kafka disabled; outbox work is persisted but not published here. Atlas's ordinary hold default remains five minutes. The project backend source was not changed.

## Evidence and design

Nine scoped real UI/API checks passed, including a competing 409 response, ownership isolation, expiration, confirmation, idempotency replay, single cancellation release and mobile width. See `../brag-output-2026-09-29-020711/evidence/api-results.json`, `capture-v2.log` and `mobile.png`. No full backend suite was rerun for this UI addition.

Design guidance: https://github.com/anthropics/skills/tree/main/skills/frontend-design, installed at `.agents/skills/frontend-design`. `DESIGN.md` records the direction. The recorded datasets remain in the `atlas-flight-demo_flight-db` and `atlas-flight-recording-v2_flight-db` volumes; the interactive app uses a separate `atlas-flight-ui_flight-db` volume.
