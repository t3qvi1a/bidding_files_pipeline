/**
 * 【模块功能】管理 Pipeline Web 表单、任务轮询、进度展示和产物下载。
 * @author gexinyan
 * @CreateTime 2026-07-16 16:20:00
 */

let currentJobId = "";
let currentJobStatus = "";
let pollTimer = null;
const JOB_STORAGE_KEY = "biddingPipelineCurrentJobId";
const TERMINAL_STATUSES = ["completed", "failed", "cancelled", "interrupted"];

/**
 * 【函数功能】读取页面元素并缩短后续 DOM 查询代码。
 * @param {string} id - DOM 元素 ID。
 * @returns {HTMLElement} 对应页面元素。
 * @throws {Error} 元素不存在时抛出。
 * @author gexinyan
 * @CreateTime 2026-07-16 16:20:00
 */
function byId(id) {
  const element = document.getElementById(id);
  if (!element) throw new Error(`页面元素不存在：${id}`);
  return element;
}


/**
 * 【函数功能】将页面展示中的技术术语转换为业务人员易理解的中文表述。
 * @param {string} value - 待转换的原始文本。
 * @returns {string} 转换后的展示文本。
 * @author gexinyan
 * @CreateTime 2026-07-30 09:15:00
 */
function humanizeDisplayText(value) {
  const replacements = [
    ["投标文件封面·tender_cover", "投标文件封面（tender_cover）"],
    ["评标报告·bid_evaluation_report", "评标报告（bid_evaluation_report）"],
    ["中标候选人公示·bid_candidates", "中标候选人公示（bid_candidates）"],
    ["中标通知书·award_notice", "中标通知书（award_notice）"],
    ["中标人公告·bidannouncement", "中标人公告（bidannouncement）"],
    ["投标单位名单·bidlist", "投标单位名单（bidlist）"],
    ["备案材料·archive_info", "备案材料（archive_info）"],
    ["tender_cover", "投标文件封面（tender_cover）"],
    ["bid_evaluation_report", "评标报告（bid_evaluation_report）"],
    ["bid_candidates", "中标候选人公示（bid_candidates）"],
    ["award_notice", "中标通知书（award_notice）"],
    ["bidannouncement", "中标人公告（bidannouncement）"],
    ["bidlist", "投标单位名单（bidlist）"],
    ["archive_info", "备案材料（archive_info）"],
    ["企业信息爬取", "工商信息获取"],
    ["企业爬虫", "工商信息获取"],
    ["爬虫", "工商信息获取"],
    ["Pipeline", "完整流程"],
    ["pipeline", "完整流程"],
    ["include/exclude", "仅处理或排除"],
    ["include", "仅处理"],
    ["exclude", "排除"],
    ["OCR", "文字识别"],
  ];
  return replacements.reduce(
    (text, [source, target]) => text.split(source).join(target),
    String(value || ""),
  );
}

/**
 * 【函数功能】将关系扩展的内部状态代码转换为中文状态。
 * @param {string} status - 接口返回的关系扩展状态代码。
 * @returns {string} 中文状态文本。
 * @author gexinyan
 * @CreateTime 2026-07-30 09:15:00
 */
function formatExpansionStatus(status) {
  const labels = {
    WAITING: "等待开始",
    RUNNING: "正在获取",
    COMPLETED: "已完成",
    FAILED: "失败",
    CANCELLED: "已中止",
  };
  return labels[status] || "等待开始";
}

/**
 * 【函数功能】加载服务器允许目录与可选文件类别。
 * @returns {Promise<void>} 配置加载完成后返回。
 * @throws {Error} 配置接口不可用时抛出。
 * @author gexinyan
 * @CreateTime 2026-07-16 16:20:00
 */
async function loadConfig() {
  const response = await fetch("/api/config");
  if (!response.ok) throw new Error("无法读取服务配置");
  const config = await response.json();
  byId("allowed-roots").textContent = `允许目录：${config.allowedInputRoots.join("；") || "未配置"}`;
  byId("category-list").innerHTML = config.categories.map((item) => (
    `<label class="category-item"><input type="checkbox" name="category" value="${item.value}">` +
    `<span>${item.label}</span></label>`
  )).join("");
  updateCategoryState();
}

/**
 * 【函数功能】根据文件来源单选项切换上传区和服务器路径区。
 * @returns {void} 无返回值。
 * @author gexinyan
 * @CreateTime 2026-07-16 16:20:00
 */
function updateSourceMode() {
  const selected = document.querySelector('input[name="source_mode"]:checked').value;
  byId("upload-source").classList.toggle("hidden", selected !== "upload");
  byId("local-source").classList.toggle("hidden", selected !== "local");
}

/**
 * 【函数功能】根据 all/include/exclude 模式启用或禁用类别复选项。
 * @returns {void} 无返回值。
 * @author gexinyan
 * @CreateTime 2026-07-16 16:20:00
 */
function updateCategoryState() {
  const disabled = byId("category-mode").value === "all";
  byId("category-list").classList.toggle("disabled", disabled);
  document.querySelectorAll('input[name="category"]').forEach((input) => { input.disabled = disabled; });
}

/**
 * 【函数功能】显示或清除表单错误提示。
 * @param {string} message - 错误文本，空字符串表示清除。
 * @returns {void} 无返回值。
 * @author gexinyan
 * @CreateTime 2026-07-16 16:20:00
 */
function showError(message) {
  const alert = byId("form-error");
  alert.textContent = message;
  alert.classList.toggle("hidden", !message);
}

/**
 * 【函数功能】提交任务表单并启动状态轮询。
 * @param {SubmitEvent} event - 表单提交事件。
 * @returns {Promise<void>} 任务创建完成后返回。
 * @author gexinyan
 * @CreateTime 2026-07-16 16:20:00
 */
async function submitJob(event) {
  event.preventDefault();
  showError("");
  const sourceMode = document.querySelector('input[name="source_mode"]:checked').value;
  const categoryMode = byId("category-mode").value;
  const selectedCategories = [...document.querySelectorAll('input[name="category"]:checked')].map((item) => item.value);
  if (sourceMode === "upload" && !byId("archive").files.length) {
    showError("请选择需要解析的压缩文件。"); return;
  }
  if (sourceMode === "local" && !byId("local-path").value.trim()) {
    showError("请输入服务器本地目录。"); return;
  }
  if (categoryMode !== "all" && !selectedCategories.length) {
    showError("仅处理或排除模式至少选择一个文件类别。"); return;
  }
  const data = new FormData();
  if (sourceMode === "upload") data.append("archive", byId("archive").files[0]);
  else data.append("local_path", byId("local-path").value.trim());
  data.append("category_mode", categoryMode);
  data.append("categories", selectedCategories.join(","));
  data.append("force_ocr", byId("force-ocr").checked ? "true" : "false");
  data.append(
    "skip_existing_company_info",
    byId("skip-existing-company-info").checked ? "true" : "false",
  );
  data.append(
    "fast_company_timeout",
    byId("fast-company-timeout").checked ? "true" : "false",
  );
  byId("start-button").disabled = true;
  try {
    const response = await fetch("/api/jobs", { method: "POST", body: data });
    const result = await response.json();
    if (!response.ok) throw new Error(result.detail || "任务创建失败");
    currentJobId = result.jobId;
    window.localStorage.setItem(JOB_STORAGE_KEY, currentJobId);
    byId("execution").classList.remove("hidden");
    byId("job-id").textContent = currentJobId;
    byId("execution").scrollIntoView({ behavior: "smooth", block: "start" });
    await beginPolling();
  } catch (error) {
    showError(error.message || String(error));
    byId("start-button").disabled = false;
  }
}

/**
 * 【函数功能】轮询当前任务状态并在终态停止定时器。
 * @returns {Promise<void>} 单次轮询完成后返回。
 * @author gexinyan
 * @CreateTime 2026-07-16 16:20:00
 */
async function pollJob() {
  if (!currentJobId) return;
  try {
    const response = await fetch(`/api/jobs/${currentJobId}`);
    if (!response.ok) throw new Error("无法读取任务状态");
    const job = await response.json();
    renderJob(job);
    if (TERMINAL_STATUSES.includes(job.status)) {
      window.clearInterval(pollTimer);
      pollTimer = null;
      byId("start-button").disabled = false;
    }
  } catch (error) {
    showError(error.message || String(error));
  }
}

/**
 * 【函数功能】立即读取一次任务状态并建立唯一的定时轮询器。
 * @returns {Promise<void>} 首次状态读取完成后返回。
 * @author gexinyan
 * @CreateTime 2026-07-16 17:40:00
 */
async function beginPolling() {
  if (pollTimer) window.clearInterval(pollTimer);
  await pollJob();
  if (currentJobId && !TERMINAL_STATUSES.includes(currentJobStatus) && !pollTimer) {
    pollTimer = window.setInterval(pollJob, 1200);
  }
}

/**
 * 【函数功能】读取指定任务；任务不存在时返回空值供恢复逻辑回退。
 * @param {string} jobId - 任务唯一标识。
 * @returns {Promise<Object|null>} 任务状态或空值。
 * @throws {Error} 非 404 接口错误时抛出。
 * @author gexinyan
 * @CreateTime 2026-07-16 17:40:00
 */
async function fetchJobState(jobId) {
  const response = await fetch(`/api/jobs/${jobId}`);
  if (response.status === 404) return null;
  if (!response.ok) throw new Error("无法恢复任务状态");
  return response.json();
}

/**
 * 【函数功能】在页面刷新后从 localStorage 或服务端最新任务恢复进度和日志。
 * @returns {Promise<void>} 恢复尝试完成后返回。
 * @author gexinyan
 * @CreateTime 2026-07-16 17:40:00
 */
async function restoreJob() {
  const storedJobId = window.localStorage.getItem(JOB_STORAGE_KEY) || "";
  let job = storedJobId ? await fetchJobState(storedJobId) : null;
  if (!job) {
    window.localStorage.removeItem(JOB_STORAGE_KEY);
    const response = await fetch("/api/jobs/latest");
    if (response.status === 404) return;
    if (!response.ok) throw new Error("无法读取最近任务");
    job = await response.json();
  }
  currentJobId = job.jobId;
  window.localStorage.setItem(JOB_STORAGE_KEY, currentJobId);
  byId("execution").classList.remove("hidden");
  byId("job-id").textContent = currentJobId;
  renderJob(job);
  if (!TERMINAL_STATUSES.includes(job.status)) {
    byId("start-button").disabled = true;
    if (pollTimer) window.clearInterval(pollTimer);
    pollTimer = window.setInterval(pollJob, 1200);
  }
}

/**
 * 【函数功能】请求服务端终止当前 Pipeline 独立进程及其 OCR 子进程。
 * @returns {Promise<void>} 中止请求处理完成后返回。
 * @author gexinyan
 * @CreateTime 2026-07-16 17:40:00
 */
async function cancelCurrentJob() {
  if (!currentJobId || !window.confirm("确定要中止当前解析任务吗？已生成的部分文件会保留。")) return;
  const button = byId("cancel-button");
  button.disabled = true;
  try {
    const response = await fetch(`/api/jobs/${currentJobId}/cancel`, { method: "POST" });
    const result = await response.json();
    if (!response.ok) throw new Error(result.detail || "中止任务失败");
    renderJob(result);
    await beginPolling();
  } catch (error) {
    showError(error.message || String(error));
    button.disabled = false;
  }
}

/**
 * 【函数功能】使用当前历史任务保存的输入与配置创建一个全新 Pipeline 任务。
 * @returns {Promise<void>} 重试任务创建并开始轮询后返回。
 * @throws {Error} 服务端拒绝重试或任务创建失败时抛出。
 * @author gexinyan
 * @CreateTime 2026-07-17 08:54:27
 */
async function retryCurrentJob() {
  if (!currentJobId) return;
  const button = byId("retry-button");
  if (!window.confirm("确定使用该任务保存的输入和运行配置重新执行吗？这会创建一个新的任务记录。")) return;
  showError("");
  button.disabled = true;
  try {
    const response = await fetch(`/api/jobs/${currentJobId}/retry`, { method: "POST" });
    const result = await response.json();
    if (!response.ok) throw new Error(result.detail || "重新执行任务失败");
    currentJobId = result.jobId;
    window.localStorage.setItem(JOB_STORAGE_KEY, currentJobId);
    byId("job-id").textContent = currentJobId;
    renderJob(result);
    await beginPolling();
  } catch (error) {
    showError(error.message || String(error));
    button.disabled = false;
  }
}

/**
 * 【函数功能】将任务状态渲染到进度、日志、阶段和下载按钮。
 * @param {Object} job - 服务端任务状态对象。
 * @returns {void} 无返回值。
 * @author gexinyan
 * @CreateTime 2026-07-16 16:20:00
 */
function renderJob(job) {
  currentJobStatus = job.status;
  byId("stage-title").textContent = humanizeDisplayText(job.stage);
  byId("progress-value").textContent = `${job.progress}%`;
  byId("progress-bar").style.width = `${job.progress}%`;
  renderPdfProgress(job.pdfProgress);
  renderSpiderProgress(job.spiderProgress);
  const badge = byId("status-badge");
  const labels = {
    queued: "排队中",
    running: "执行中",
    cancelling: "正在中止",
    completed: "已完成",
    failed: "失败",
    cancelled: "已中止",
    interrupted: "已中断",
  };
  badge.textContent = labels[job.status] || job.status;
  const badgeClass = job.status === "completed" ? "success" :
    job.status === "failed" || job.status === "interrupted" ? "failed" :
      job.status === "cancelled" ? "cancelled" : "";
  badge.className = `status-badge ${badgeClass}`;
  const retryButton = byId("retry-button");
  retryButton.disabled = !job.canRetry;
  retryButton.title = job.canRetry ? `使用 ${job.inputSummary} 重新执行` : (job.retryReason || "当前任务不可重试");
  const retryHint = byId("retry-hint");
  const retryHintText = job.canRetry ?
    `可使用“${job.inputSummary}”及保存的运行配置创建新任务。` :
    (TERMINAL_STATUSES.includes(job.status) ? job.retryReason : "");
  retryHint.textContent = retryHintText;
  retryHint.classList.toggle("hidden", !retryHintText);
  byId("cancel-button").disabled = !["queued", "running"].includes(job.status);
  byId("start-button").disabled = !TERMINAL_STATUSES.includes(job.status);
  const thresholds = [8, 38, 62, 76, 90];
  document.querySelectorAll(".stage-list li").forEach((item, index) => {
    item.classList.toggle("active", job.progress >= thresholds[index]);
  });
  const log = byId("log-output");
  const shouldStick = log.scrollHeight - log.scrollTop - log.clientHeight < 50;
  log.textContent = (job.logs || []).map(humanizeDisplayText).join("\n") || "任务正在初始化…";
  if (shouldStick) log.scrollTop = log.scrollHeight;
  updateArtifact("download-csv", "csv", job.artifacts);
  updateArtifact("download-report", "risk_report", job.artifacts);
  updateArtifact("download-json", "risk_json", job.artifacts);
  updateArtifact("download-log", "log", job.artifacts);
  if (job.error) showError(job.error);
}

/**
 * 【函数功能】渲染由服务端结构化事件驱动的 PDF 文件解析进度。
 * @param {Object} progress - PDF 进度对象。
 * @returns {void} 无返回值。
 * @author gexinyan
 * @CreateTime 2026-07-17 10:30:00
 */
function renderPdfProgress(progress) {
  const current = progress || {};
  const total = Math.max(0, Number(current.total) || 0);
  const completed = Math.min(Math.max(0, Number(current.completed) || 0), total);
  const percent = total > 0 ? Math.floor(completed * 100 / total) : 0;
  byId("pdf-progress-value").textContent = `${percent}%`;
  byId("pdf-progress-bar").style.width = `${percent}%`;
  byId("pdf-progress-detail").textContent = total > 0 ?
    `已完成 ${completed} / ${total} 份文件` : "等待开始解析";
}

/**
 * 【函数功能】渲染企业发现数量动态变化的单线程爬虫进度。
 * @param {Object} progress - 爬虫进度对象。
 * @returns {void} 无返回值。
 * @author gexinyan
 * @CreateTime 2026-07-17 10:30:00
 */
function renderSpiderProgress(progress) {
  const current = progress || {};
  const root = current.root || {};
  const related = current.related || {};
  const rootTotal = Math.max(0, Number(root.total) || 0);
  const relatedTotal = Math.max(0, Number(related.total) || 0);
  const discovered = rootTotal + relatedTotal || Math.max(0, Number(current.discovered) || 0);
  const rootFinished = Math.min(rootTotal, (Number(root.success) || 0) + (Number(root.failed) || 0) + (Number(root.existing) || 0) + (Number(root.pending) || 0));
  const relatedFinished = Math.min(relatedTotal, (Number(related.success) || 0) + (Number(related.failed) || 0) + (Number(related.existing) || 0) + (Number(related.pending) || 0));
  const completed = rootTotal + relatedTotal > 0 ? rootFinished + relatedFinished : Math.min(Math.max(0, Number(current.completed) || 0), discovered);
  const running = Math.max(0, Number(current.running) || 0);
  const failed = Math.max(0, Number(current.failed) || 0);
  const percent = discovered > 0 ? Math.floor(completed * 100 / discovered) : 0;
  const rootPercent = rootTotal > 0 ? Math.floor(rootFinished * 100 / rootTotal) : 0;
  const relatedPercent = relatedTotal > 0 ? Math.floor(relatedFinished * 100 / relatedTotal) : 0;
  const phase = current.phase || "waiting_for_companies";
  const pending = Math.max(0, (Number(root.pending) || 0) + (Number(related.pending) || 0));
  const existing = Math.max(0, (Number(root.existing) || 0) + (Number(related.existing) || 0));
  const existingDataOnly = phase === "existing_data_only";
  const spiderFinished = phase === "completed" || existingDataOnly || phase === "failed";
  const displayPercent = existingDataOnly ? 100 : percent;
  const displayRelatedPercent = spiderFinished ? 100 : relatedPercent;
  let detail = `已完成工商信息获取 ${completed} / 已发现企业 ${discovered} 家 · 正在处理 ${running} 家 · 失败 ${failed} 家 · 待核验 ${pending} 家 · 数据已存在 ${existing} 家企业`;
  if (discovered === 0 && phase === "waiting_for_companies") {
    detail = "等待文件解析完成，尚未发现企业";
  } else if (discovered === 0 && phase === "completed") {
    detail = "未发现需要获取工商信息的企业";
  } else if (existingDataOnly) {
    detail = `已使用数据库已有工商信息，跳过获取；数据已存在 ${existing} 家企业`;
  }
  byId("spider-progress-value").textContent = `${displayPercent}%`;
  byId("spider-progress-bar").style.width = `${displayPercent}%`;
  byId("spider-progress-detail").textContent = detail;
  byId("root-progress-value").textContent = `${rootPercent}%`;
  byId("root-progress-bar").style.width = `${rootPercent}%`;
  byId("root-progress-detail").textContent = `参与投标企业：${rootTotal} 家｜已完成：${Number(root.success) || 0}｜失败：${Number(root.failed) || 0}｜待核验：${Number(root.pending) || 0}｜已有数据：${Number(root.existing) || 0}`;
  byId("related-progress-value").textContent = `${displayRelatedPercent}%`;
  byId("related-progress-bar").style.width = `${displayRelatedPercent}%`;
  const expansionStatus = current.expansionStatus || "WAITING";
  if (spiderFinished && relatedTotal === 0) {
    byId("related-progress-detail").textContent = expansionStatus === "FAILED" ?
      "未发现可处理的相关企业，关联关系扩展已失败并结束" :
      "未发现可处理的相关企业，关联关系扩展已结束";
  } else {
    byId("related-progress-detail").textContent = `已发现相关企业：${relatedTotal} 家｜已完成：${Number(related.success) || 0}｜失败：${Number(related.failed) || 0}｜待核验：${Number(related.pending) || 0}｜已有数据：${Number(related.existing) || 0}`;
  }
  byId("expansion-status-detail").textContent = `关系扩展状态：${formatExpansionStatus(expansionStatus)}`;
  byId("related-progress-section").classList.toggle("hidden", existingDataOnly);
}

/**
 * 【函数功能】根据产物状态启用或禁用指定下载按钮。
 * @param {string} elementId - 下载按钮元素 ID。
 * @param {string} artifactName - 后端产物键名。
 * @param {string[]} artifacts - 当前已生成的产物列表。
 * @returns {void} 无返回值。
 * @author gexinyan
 * @CreateTime 2026-07-16 16:20:00
 */
function updateArtifact(elementId, artifactName, artifacts) {
  const element = byId(elementId);
  const available = artifacts.includes(artifactName);
  element.classList.toggle("disabled", !available);
  element.setAttribute("aria-disabled", String(!available));
  element.href = available ? `/api/jobs/${currentJobId}/artifacts/${artifactName}` : "#";
}

/**
 * 【函数功能】更新上传框中显示的压缩包文件名。
 * @returns {void} 无返回值。
 * @author gexinyan
 * @CreateTime 2026-07-16 16:20:00
 */
function updateUploadLabel() {
  const file = byId("archive").files[0];
  byId("upload-label").textContent = file ? file.name : "选择投标文件压缩包";
}

/**
 * 【函数功能】注册页面事件并加载初始服务配置。
 * @returns {Promise<void>} 页面初始化完成后返回。
 * @author gexinyan
 * @CreateTime 2026-07-16 16:20:00
 */
async function initializePage() {
  document.querySelectorAll('input[name="source_mode"]').forEach((input) => input.addEventListener("change", updateSourceMode));
  byId("category-mode").addEventListener("change", updateCategoryState);
  byId("archive").addEventListener("change", updateUploadLabel);
  byId("job-form").addEventListener("submit", submitJob);
  byId("retry-button").addEventListener("click", retryCurrentJob);
  byId("cancel-button").addEventListener("click", cancelCurrentJob);
  try {
    await loadConfig();
    await restoreJob();
  } catch (error) {
    showError(error.message || String(error));
  }
}

document.addEventListener("DOMContentLoaded", initializePage);
