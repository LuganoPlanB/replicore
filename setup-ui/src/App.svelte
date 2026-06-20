<script>
  import { onMount } from "svelte"

  const emptyDraft = {
    selectedInterface: "",
    bindHost: "",
    clusterSecret: "",
    machineIdentity: "",
    machineId: ""
  }

  let setupState = null
  let interfaces = []
  let draft = { ...emptyDraft }
  let loading = true
  let loadingError = ""
  let deriveError = ""
  let saveError = ""
  let saveMessage = ""
  let isSaving = false
  let isDeriving = false

  onMount(() => {
    void loadWizard()
  })

  async function loadWizard() {
    loading = true
    loadingError = ""

    try {
      const [stateResponse, interfacesResponse, draftResponse] = await Promise.all([
        readJson("/setup/state"),
        readJson("/setup/interfaces"),
        readOptionalDraft()
      ])

      setupState = stateResponse
      interfaces = interfacesResponse.interfaces ?? []

      if (draftResponse?.draft) {
        draft = { ...emptyDraft, ...draftResponse.draft }
      } else {
        draft = {
          ...emptyDraft,
          selectedInterface: interfaces[0]?.name ?? "",
          bindHost: firstEligibleAddress(interfaces) ?? ""
        }
      }

      syncBindHostFromSelection()

      if (draft.clusterSecret && draft.machineIdentity) {
        await refreshMachineId()
      }
    } catch (error) {
      loadingError = error.message
    } finally {
      loading = false
    }
  }

  function firstEligibleAddress(records) {
    return records.find((record) => record.eligibleForBind)?.address ?? ""
  }

  function selectedInterfaceRecords() {
    return interfaces.filter((record) => record.name === draft.selectedInterface)
  }

  function selectedBindCandidates() {
    return selectedInterfaceRecords().filter((record) => record.eligibleForBind)
  }

  function syncBindHostFromSelection() {
    const match = selectedBindCandidates().find((record) => record.address === draft.bindHost)

    if (match) return

    draft = {
      ...draft,
      bindHost: selectedBindCandidates()[0]?.address ?? ""
    }
  }

  async function handleInterfaceChange(event) {
    draft = {
      ...draft,
      selectedInterface: event.currentTarget.value
    }

    syncBindHostFromSelection()
  }

  async function handleSecretInput(event) {
    draft = {
      ...draft,
      clusterSecret: event.currentTarget.value
    }

    await refreshMachineId()
  }

  async function handleMachineIdentityInput(event) {
    draft = {
      ...draft,
      machineIdentity: event.currentTarget.value
    }

    await refreshMachineId()
  }

  async function refreshMachineId() {
    deriveError = ""
    saveMessage = ""

    if (!draft.clusterSecret.trim() || !draft.machineIdentity.trim()) {
      draft = {
        ...draft,
        machineId: ""
      }
      return
    }

    isDeriving = true

    try {
      const response = await requestJson("/setup/derive-machine-id", {
        method: "POST",
        body: {
          clusterSecret: draft.clusterSecret,
          machineIdentity: draft.machineIdentity
        }
      })

      draft = {
        ...draft,
        clusterSecret: draft.clusterSecret.trim().toLowerCase(),
        machineIdentity: draft.machineIdentity.trim(),
        machineId: response.machineId
      }
    } catch (error) {
      draft = {
        ...draft,
        machineId: ""
      }
      deriveError = error.message
    } finally {
      isDeriving = false
    }
  }

  async function saveDraft() {
    saveError = ""
    saveMessage = ""
    isSaving = true

    try {
      const response = await requestJson("/setup/draft", {
        method: "POST",
        body: draft
      })

      draft = { ...draft, ...response.draft }
      saveMessage = "Draft saved locally."
    } catch (error) {
      saveError = error.message
    } finally {
      isSaving = false
    }
  }

  async function readOptionalDraft() {
    try {
      return await readJson("/setup/draft")
    } catch (error) {
      if (error.status === 404 || error.status === 409) {
        return null
      }

      throw error
    }
  }

  async function readJson(url) {
    return requestJson(url)
  }

  async function requestJson(url, options = {}) {
    const response = await fetch(url, {
      method: options.method ?? "GET",
      headers: options.body === undefined ? {} : { "content-type": "application/json" },
      body: options.body === undefined ? undefined : JSON.stringify(options.body)
    })

    const payload = await response.json()

    if (!response.ok) {
      const error = new Error(payload.error ?? `Request failed with status ${response.status}`)
      error.status = response.status
      throw error
    }

    return payload
  }
</script>

<svelte:head>
  <title>Replicore Setup</title>
</svelte:head>

{#if loading}
  <main class="shell">
    <section class="panel">
      <p class="eyebrow">Replicore</p>
      <h1>Node setup</h1>
      <p class="summary">Loading local setup state…</p>
    </section>
  </main>
{:else if loadingError}
  <main class="shell">
    <section class="panel">
      <p class="eyebrow">Replicore</p>
      <h1>Node setup</h1>
      <p class="error" role="alert">{loadingError}</p>
    </section>
  </main>
{:else}
  <main class="shell">
    <section class="panel" aria-labelledby="setup-title">
      <header class="header">
        <div>
          <p class="eyebrow">Replicore</p>
          <h1 id="setup-title">Node setup</h1>
        </div>
        <dl class="meta">
          <div>
            <dt>Mode</dt>
            <dd>{setupState?.mode ?? "setup"}</dd>
          </div>
          <div>
            <dt>Config file</dt>
            <dd>{setupState?.configPath ?? "Not selected"}</dd>
          </div>
        </dl>
      </header>

      <form class="wizard" on:submit|preventDefault={saveDraft}>
        <section class="group" aria-labelledby="network-title">
          <div class="group-header">
            <h2 id="network-title">Network</h2>
          </div>

          <label class="field">
            <span>Interface</span>
            <select bind:value={draft.selectedInterface} on:change={handleInterfaceChange}>
              {#each Array.from(new Set(interfaces.map((record) => record.name))) as name}
                <option value={name}>{name}</option>
              {/each}
            </select>
          </label>

          <label class="field">
            <span>Bind host</span>
            <input type="text" bind:value={draft.bindHost} readonly />
          </label>

          <div class="interface-list" role="table" aria-label="Discovered addresses">
            <div class="interface-head" role="row">
              <span role="columnheader">Family</span>
              <span role="columnheader">Address</span>
              <span role="columnheader">Bind</span>
            </div>
            {#each selectedInterfaceRecords() as record}
              <div class="interface-row" role="row">
                <span role="cell">{record.family}</span>
                <span class="code-cell" role="cell">{record.address}</span>
                <span role="cell">{record.eligibleForBind ? "Yes" : "No"}</span>
              </div>
            {/each}
          </div>
        </section>

        <section class="group" aria-labelledby="identity-title">
          <div class="group-header">
            <h2 id="identity-title">Cluster identity</h2>
          </div>

          <label class="field">
            <span>Cluster secret</span>
            <input
              type="password"
              value={draft.clusterSecret}
              on:input={handleSecretInput}
              autocomplete="off"
              spellcheck="false"
            />
          </label>

          <label class="field">
            <span>Machine identity</span>
            <textarea
              rows="4"
              value={draft.machineIdentity}
              on:input={handleMachineIdentityInput}
              spellcheck="false"
            ></textarea>
          </label>

          <label class="field">
            <span>Derived machine identifier</span>
            <textarea rows="3" bind:value={draft.machineId} readonly></textarea>
          </label>

          {#if deriveError}
            <p class="error" role="alert">{deriveError}</p>
          {:else if isDeriving}
            <p class="status">Deriving machine identifier…</p>
          {/if}
        </section>

        <footer class="actions">
          <button type="submit" disabled={isSaving || !draft.machineId}>Save draft</button>
          {#if saveError}
            <p class="error" role="alert">{saveError}</p>
          {:else if saveMessage}
            <p class="status">{saveMessage}</p>
          {/if}
        </footer>
      </form>
    </section>
  </main>
{/if}
