# Put your code here if python is chosen for this project.


 


import threading
import time
from kubernetes import client, config, watch
from kubernetes.client.rest import ApiException

# ---- Settings ---------------------------------------------------------------
GROUP = "cosc3657.tu.ca"           # from crd_definition.yaml
VERSION = "v1alpha1"
PLURAL = "resilientdeployments"
NAMESPACE = "default"
LABEL = "app.kubernetes.io/managed-by"   # label we put on every pod we create

# ---- Connect to the cluster (same login kubectl uses) -----------------------
config.load_kube_config()
pods_api = client.CoreV1Api()            # talks to Pods
custom_api = client.CustomObjectsApi()   # talks to our ResilientDeployments

last_drift = {}  # remembers the last drift per resource, so we only print changes


# ---- STEP 1a: Desired state -------------------------------------------------
def get_resilient_deployments():
    """Read every ResilientDeployment the user has applied."""
    result = custom_api.list_namespaced_custom_object(GROUP, VERSION, NAMESPACE, PLURAL)
    return result["items"]


# ---- STEP 1b: Observed state ------------------------------------------------
def get_live_pods(name):
    """Find the pods that belong to this resource and are alive."""
    pods = pods_api.list_namespaced_pod(NAMESPACE, label_selector=f"{LABEL}={name}").items
    live = []
    for pod in pods:
        being_deleted = pod.metadata.deletion_timestamp is not None
        # Pending counts as alive: it's starting up. If we only counted Running,
        # we'd create duplicate pods while the image is still downloading.
        alive = pod.status.phase in ("Pending", "Running")
        if alive and not being_deleted:
            live.append(pod)
    return live


# ---- Building and removing pods ---------------------------------------------
def create_pod(rd):
    """Create one pod for this ResilientDeployment."""
    name = rd["metadata"]["name"]
    image = rd["spec"]["image"]
    # http-echo needs a -text argument or it exits immediately
    args = [f"-text=hello from {name}"] if "http-echo" in image else None

    pod = client.V1Pod(
        metadata=client.V1ObjectMeta(
            generate_name=f"{name}-",     # Kubernetes adds a random suffix
            labels={LABEL: name},          # so we can find this pod later
            owner_references=[client.V1OwnerReference(   # deleting the resource
                api_version=f"{GROUP}/{VERSION}",         # deletes its pods too
                kind="ResilientDeployment",
                name=name,
                uid=rd["metadata"]["uid"],
            )],
        ),
        spec=client.V1PodSpec(
            containers=[client.V1Container(name="app", image=image, args=args)],
        ),
    )
    created = pods_api.create_namespaced_pod(NAMESPACE, pod)
    print(f"  + created {created.metadata.name}")


def delete_pod(pod):
    try:
        pods_api.delete_namespaced_pod(pod.metadata.name, NAMESPACE)
        print(f"  - deleted {pod.metadata.name}")
    except ApiException as e:
        if e.status != 404:   # 404 = already gone, which is what we wanted anyway
            raise


# ---- STEPS 2 + 3: Drift and Reconcile ---------------------------------------
def reconcile(rd):
    name = rd["metadata"]["name"]
    desired = rd["spec"]["replicas"]
    live_pods = get_live_pods(name)
    observed = len(live_pods)

    drift = desired - observed

    if drift != 0:
        print(f"[RECONCILE LOOP] Resource: {name} | Desired: {desired} | "
              f"Observed: {observed} | Drift: {drift}")

    if drift > 0:
        print(f"Scale-Up Event: Provisioning {drift} pod(s) to resolve drift...")
        for _ in range(drift):
            create_pod(rd)

    elif drift < 0:
        print(f"Scale-Down Event: Pruning {-drift} excess pod(s)...")
        for pod in live_pods[:-drift]:
            delete_pod(pod)

    elif last_drift.get(name) != 0:
        print(f"EQUILIBRIUM REACHED: System state matches target spec for '{name}'.")

    last_drift[name] = drift


# ---- Watches: the API server pushes events to us ----------------------------
# Instead of constantly asking "did anything change?", we open a watch and the
# API server tells us the moment something happens. Each watch runs in its own
# background thread and "rings the bell" (wake_up) so the main loop reacts now.
wake_up = threading.Event()


def watch_pods():
    """Watch 1: our pods (created, crashed, deleted)."""
    while True:
        try:
            for event in watch.Watch().stream(pods_api.list_namespaced_pod, NAMESPACE,
                                              label_selector=LABEL):
                if event["type"] == "DELETED":
                    print(f"[WATCH] Pod {event['object'].metadata.name} was deleted!")
                wake_up.set()
        except Exception as e:
            print(f"[WATCH ERROR] pods: {e} - reconnecting...")
            time.sleep(2)


def watch_resilient_deployments():
    """Watch 2: our custom resource (applied, patched, deleted)."""
    while True:
        try:
            for event in watch.Watch().stream(custom_api.list_namespaced_custom_object,
                                              GROUP, VERSION, NAMESPACE, PLURAL):
                name = event["object"]["metadata"]["name"]
                print(f"[WATCH] ResilientDeployment {name} {event['type']}")
                wake_up.set()
        except Exception as e:
            print(f"[WATCH ERROR] resources: {e} - reconnecting...")
            time.sleep(2)


# ---- Main loop --------------------------------------------------------------
def main():
    print("=== COSC 3657 Custom Kubernetes Reconciler Started ===")

    # Start both watches in the background
    threading.Thread(target=watch_pods, daemon=True).start()
    threading.Thread(target=watch_resilient_deployments, daemon=True).start()

    while True:
        try:
            wake_up.clear()
            for rd in get_resilient_deployments():
                reconcile(rd)
            # Sleep until a watch rings the bell, or 2 seconds as a safety net
            wake_up.wait(timeout=2)
        except KeyboardInterrupt:
            print("Stopping reconciler.")
            break
        except Exception as e:
            # API hiccup? Don't crash - just try again next loop.
            print(f"[ERROR] {e} - retrying...")
            time.sleep(2)


if __name__ == "__main__":
    main()