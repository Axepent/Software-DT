import * as THREE from 'three';
import { OrbitControls } from 'three/addons/controls/OrbitControls.js';
import { GLTFLoader } from 'three/addons/loaders/GLTFLoader.js';

const colors = {
  normal: new THREE.Color('#52c6a7'),
  warning: new THREE.Color('#ffb945'),
  emergency: new THREE.Color('#ff435e'),
  unknown: new THREE.Color('#a4afbd'),
};

export async function createScene(viewport, bindings, select, frame) {
  const { scene: model } = await new GLTFLoader().loadAsync('/assets/models/mannequin-lod1.glb');
  const scene = new THREE.Scene();
  scene.background = new THREE.Color('#171f2c');
  scene.add(model);
  const bounds = new THREE.Box3().setFromObject(model);
  const center = bounds.getCenter(new THREE.Vector3());

  const renderer = new THREE.WebGLRenderer({ antialias: true });
  renderer.setPixelRatio(Math.min(window.devicePixelRatio || 1, 1.5));
  viewport.append(renderer.domElement);
  const camera = new THREE.PerspectiveCamera(38, 1, 0.02, 40);
  const controls = new OrbitControls(camera, renderer.domElement);
  controls.enableDamping = true;
  controls.minDistance = 0.38;
  controls.maxDistance = 8;
  controls.maxPolarAngle = Math.PI * 0.52;

  scene.add(new THREE.HemisphereLight(0xe4efff, 0x51637a, 2.05));
  const key = new THREE.DirectionalLight(0xffebd5, 2.5);
  key.position.set(2, 4, 4);
  const fill = new THREE.DirectionalLight(0x92c5ff, 1.8);
  fill.position.set(-3, 2, -2);
  scene.add(key, fill);
  const grid = new THREE.GridHelper(12, 48, 0x566477, 0x303c4d);
  grid.position.y = bounds.min.y - 0.015;
  scene.add(grid);

  const pivots = new Map();
  const staticMaterials = [];
  model.traverse(node => {
    const binding = bindings.find(item => item.node === node.userData.element);
    if (binding) {
      const axis = new THREE.Vector3();
      axis[binding.axis] = 1;
      pivots.set(binding.sensor, {
        node, axis, base: node.quaternion.clone(), direction: binding.direction,
        angle: 0, target: 0, materials: [],
      });
    }
    if (!node.isMesh) return;
    let owner = node.parent;
    while (owner && !owner.userData.element) owner = owner.parent;
    const ownerBinding = bindings.find(item => item.node === owner?.userData.element);
    const pivot = pivots.get(ownerBinding?.sensor);
    node.material = node.material.clone();
    node.material.roughness = 0.62;
    const surface = { material: node.material, baseColor: node.material.color.clone() };
    if (pivot) {
      node.userData.sensor = ownerBinding.sensor;
      pivot.materials.push(surface);
    } else {
      staticMaterials.push(surface);
    }
  });
  if (pivots.size !== bindings.length || [...pivots.values()].some(pivot => !pivot.materials.length)) {
    throw new Error('Вузли GLB не відповідають конфігурації прив’язки.');
  }

  const offset = new THREE.Vector3(1.35, 0.5, 3.3);
  const focus = new THREE.Vector3();
  const focusStep = new THREE.Vector3();
  let active = null;
  let focusing = false;
  let level = 'unknown';

  function reset() {
    active = null;
    focusing = false;
    controls.target.copy(center);
    const portraitScale = camera.aspect < 0.78 ? 0.78 / camera.aspect : 1;
    camera.position.copy(center).addScaledVector(offset, portraitScale);
    controls.update();
  }

  function focusOn(sensor) {
    active = sensor;
    focusing = Boolean(sensor);
  }

  const size = () => {
    const { clientWidth: width, clientHeight: height } = viewport;
    renderer.setSize(width, height, false);
    camera.aspect = width / Math.max(1, height);
    camera.updateProjectionMatrix();
  };
  new ResizeObserver(size).observe(viewport);
  size();
  reset();
  controls.addEventListener('start', () => { focusing = false; });

  const ray = new THREE.Raycaster();
  const pointer = new THREE.Vector2();
  let down;
  renderer.domElement.addEventListener('pointerdown', event => {
    down = event.isPrimary && event.button === 0 ? [event.clientX, event.clientY] : null;
  });
  renderer.domElement.addEventListener('pointerup', event => {
    if (!down || !event.isPrimary || Math.hypot(event.clientX - down[0], event.clientY - down[1]) > 5) return;
    const rect = renderer.domElement.getBoundingClientRect();
    pointer.set(2 * (event.clientX - rect.left) / rect.width - 1,
      1 - 2 * (event.clientY - rect.top) / rect.height);
    ray.setFromCamera(pointer, camera);
    const hit = ray.intersectObject(model, true)[0];
    select(hit?.object.userData.sensor ?? null);
  });

  function update(state, nextLevel) {
    level = nextLevel;
    for (const [sensor, pivot] of pivots) {
      const angle = state?.joint_angles[sensor];
      const quality = state?.sensor_quality?.[sensor]?.status;
      if (Number.isFinite(angle) && nextLevel !== 'unknown' && ['fresh', 'held', 'latched'].includes(quality)) {
        pivot.target = THREE.MathUtils.degToRad(angle) * pivot.direction;
      }
      const channelLevel = level === 'normal' && !['fresh', 'held', 'latched'].includes(quality)
        ? 'unknown' : level;
      for (const { material, baseColor } of pivot.materials) {
        material.color.copy(baseColor).lerp(colors[channelLevel], channelLevel === 'unknown' ? 0 : 0.58);
      }
    }
    for (const { material, baseColor } of staticMaterials) {
      material.color.copy(baseColor).lerp(colors[level], level === 'warning' || level === 'emergency' ? 0.4 : 0);
    }
  }

  const rotation = new THREE.Quaternion();
  let before = performance.now();
  function render(now) {
    const delta = Math.min((now - before) / 1000, 0.1);
    before = now;
    for (const [sensor, pivot] of pivots) {
      pivot.angle = THREE.MathUtils.damp(pivot.angle, pivot.target, 16, delta);
      pivot.node.quaternion.copy(pivot.base).multiply(rotation.setFromAxisAngle(pivot.axis, pivot.angle));
      for (const { material } of pivot.materials) {
        material.emissive.set(sensor === active ? '#a2ceff' : '#000000');
        material.emissiveIntensity = sensor === active ? 0.23 : 0;
      }
    }
    if (focusing && active) {
      pivots.get(active).node.getWorldPosition(focus);
      focusStep.subVectors(focus, controls.target).multiplyScalar(1 - Math.exp(-delta * 9));
      camera.position.add(focusStep);
      controls.target.add(focusStep);
    }
    controls.update(delta);
    frame(now);
    renderer.render(scene, camera);
  }
  renderer.setAnimationLoop(render);
  return { update, focusOn, reset };
}
