// Execute the self-contained report without a browser or external dependencies.
const fs=require('fs'),vm=require('vm'),assert=require('assert');
const html=fs.readFileSync(process.argv[2],'utf8');
const script=html.match(/<script>([\s\S]*?)<\/script>/)[1];
const els={};
function element(id){
 if(!els[id])els[id]={value:id==='#metric'?'seconds':'',checked:false,_html:'',
  set innerHTML(s){this._html=s;if(['#flow','#scenario'].includes(id)){const m=s.match(/<option value="([^"]*)"/);this.value=m?m[1]:'';}},
  get innerHTML(){return this._html;},textContent:''};
 return els[id];
}
const context=vm.createContext({document:{querySelector:element}});
vm.runInContext(script,context);
const flows=vm.runInContext('[...new Set(S.exploratory_cases.map(r=>r.workflow))]',context);
for(const flow of flows){
 element('#flow').value=flow;vm.runInContext('scenarios()',context);
 assert(element('#scenario').value);
 for(const metric of ['seconds','speedup']){
  element('#metric').value=metric;vm.runInContext('draw()',context);
  assert(element('#grid').innerHTML.includes('Complete traversal'));
  assert(!/NaN|undefined/.test(element('#grid').innerHTML));
 }
}
assert.equal(vm.runInContext("measure([{configuration:'x',location:'1',status:'completed',seconds:1,expected_batches:2}],'daily_sequence','x','TOTAL').seconds",context),null);
assert.equal(vm.runInContext("measure([{configuration:'x',location:'1',status:'completed',seconds:1,expected_batches:1}],'daily_sequence','x','TOTAL').seconds",context),1);
console.log('Report workflow filters, AD ratios and complete-only totals passed');
